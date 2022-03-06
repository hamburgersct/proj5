package surfstore

import (
	"io/ioutil"
	"log"
	"os"
)

// Implement the logic for a client syncing with the server here.
func ClientSync(client RPCClient) {
	// get block store addr
	var blockStoreAddr string
	if err := client.GetBlockStoreAddr(&blockStoreAddr); err != nil {
		log.Fatal(err)
	}

	blockSize := client.BlockSize
	indexPath := ConcatPath(client.BaseDir, DEFAULT_META_FILENAME)
	if indexExist := FileExists(indexPath); !indexExist {
		_, err := os.Create(indexPath)
		check(err)
	}
	// filename -> local hashes
	newLocalFile := make(map[string][]string)
	updatedLocalFile := make(map[string][]string)
	localMeta, err := LoadMetaFromMetaFile(client.BaseDir)
	check(err)
	allFiles, err := ioutil.ReadDir(client.BaseDir)
	check(err)
	for _, localFile := range allFiles {
		localFileName := localFile.Name()
		if localFileName != DEFAULT_META_FILENAME && !localFile.IsDir() {
			var localHashes []string
			bytes, err := ioutil.ReadFile(ConcatPath(client.BaseDir, localFileName))
			check(err)
			blockCnt := int(localFile.Size()) / blockSize
			for i := 0; i < blockCnt; i++ {
				tempBytes := bytes[i*blockSize : (i+1)*blockSize]
				localHashes = append(localHashes, GetBlockHashString(tempBytes))
			}
			if localFile.Size()%int64(client.BlockSize) != 0 {
				lastBlkStartIdx := blockCnt * int(blockSize)
				tempBytes := bytes[lastBlkStartIdx:]
				localHashes = append(localHashes, GetBlockHashString(tempBytes))
			}
			// new file
			if _, ok := localMeta[localFileName]; !ok {
				newLocalFile[localFileName] = localHashes
				localMeta[localFileName] = &FileMetaData{Filename: localFileName,
					Version:       1,
					BlockHashList: localHashes}
			} else {
				// file updated
				if equal := testEqHashes(localMeta[localFileName].GetBlockHashList(), localHashes); !equal {
					updatedLocalFile[localFileName] = localHashes
					localMeta[localFileName] = &FileMetaData{Filename: localFileName,
						Version:       localMeta[localFileName].GetVersion() + 1,
						BlockHashList: localHashes}
				}
			}

		}
	}
	// handle local deleted file
	for localFileName := range localMeta {
		if _, err := os.Stat(ConcatPath(client.BaseDir, localFileName)); os.IsNotExist(err) {
			localHashList := localMeta[localFileName].BlockHashList
			if !(len(localHashList) == 1 && localHashList[0] == "0") {
				localMeta[localFileName].Version++
				localMeta[localFileName].BlockHashList = []string{"0"}
			}
		}
	}

	// Get remote index
	var remoteIndex map[string]*FileMetaData
	err = client.GetFileInfoMap(&remoteIndex)
	check(err)

	for rmFileName, rmFileMeta := range remoteIndex {
		// remote has file not in baseDir
		remoteFileVersion := rmFileMeta.GetVersion()
		remoteHashes := rmFileMeta.GetBlockHashList()
		if _, ok := localMeta[rmFileName]; !ok {
			if !(len(remoteHashes) == 1 && remoteHashes[0] == "0") { // non-delete case
				// download from block store and write to local
				var data []byte
				//log.Println("remote hashes length: ", len(remoteHashes))
				for i := 0; i < len(remoteHashes); i++ {
					tempBlk := &Block{}
					client.GetBlock(remoteHashes[i], blockStoreAddr, tempBlk)
					data = append(data, tempBlk.GetBlockData()...)
				}
				f, err := os.Create(ConcatPath(client.BaseDir, rmFileName))
				check(err)
				defer f.Close()
				_, err = f.Write(data)
				check(err)
				// update local index
				localMeta[rmFileName] = &FileMetaData{Filename: rmFileName,
					Version:       remoteFileVersion,
					BlockHashList: remoteHashes}
			}
		} else { // remote has file in local meta
			// check version
			localFileVersion := localMeta[rmFileName].GetVersion()
			localHashes := localMeta[rmFileName].GetBlockHashList()
			// remote version higher: download, update local index
			if remoteFileVersion > localFileVersion {
				// delete file case
				if len(remoteHashes) == 1 && remoteHashes[0] == "0" {
					err := os.Remove(ConcatPath(client.BaseDir, rmFileName))
					check(err)
					localMeta[rmFileName] = rmFileMeta
				} else { // normal case
					var bytes []byte
					for _, s := range remoteHashes {
						getBlk := &Block{}
						err := client.GetBlock(s, blockStoreAddr, getBlk)
						check(err)
						bytes = append(bytes, getBlk.BlockData...)
					}
					// if deleted, then no more delete needed
					if !(len(localHashes) == 1 && localHashes[0] == "0") {
						err := os.Remove(ConcatPath(client.BaseDir, rmFileName))
						check(err)
					}
					f, err := os.Create(ConcatPath(client.BaseDir, rmFileName))
					check(err)
					defer f.Close()
					_, err = f.Write(bytes)
					check(err)
					localMeta[rmFileName] = rmFileMeta
				}
			} else if remoteFileVersion+1 == localFileVersion { // local is new
				if !(len(localHashes) == 1 && localHashes[0] == "0") { // not delete file: put needed block
					var existedBlockHashes []string
					err := client.HasBlocks(localHashes, blockStoreAddr, &existedBlockHashes)
					check(err)
					bytes, err := ioutil.ReadFile(ConcatPath(client.BaseDir, rmFileName))
					check(err)
					for i, s := range localHashes {
						if exist := stringExist(s, existedBlockHashes); !exist {
							putBlk := &Block{}
							var succ bool
							if i == len(localHashes)-1 {
								putBlk.BlockData = bytes[i*blockSize:]
								putBlk.BlockSize = int32(len(bytes) - i*blockSize)
							} else {
								putBlk.BlockData = bytes[i*blockSize : (i+1)*blockSize]
								putBlk.BlockSize = int32(blockSize)
							}
							err := client.PutBlock(putBlk, blockStoreAddr, &succ)
							check(err)
							if !succ {
								log.Fatal("fail to put block when remote has file in local meta")
							}
						}
					}
				} else { // delete file: update meta
					err := client.UpdateFile(localMeta[rmFileName], &localFileVersion)
					check(err)
				}
			}
		}
	}

	for filename, localHashes := range newLocalFile {
		if _, ok := remoteIndex[filename]; !ok {
			// put block
			bytes, err := ioutil.ReadFile(ConcatPath(client.BaseDir, filename))
			check(err)
			totalPutSize := 0
			for i := range localHashes {
				putBlk := &Block{}
				succ := false
				if i == len(localHashes)-1 {
					putBlk.BlockData = bytes[i*blockSize:]
					putBlk.BlockSize = int32(len(bytes) - i*blockSize)
				} else {
					putBlk.BlockData = bytes[i*blockSize : (i+1)*blockSize]
					putBlk.BlockSize = int32(blockSize)
				}
				totalPutSize += len(putBlk.BlockData)
				err := client.PutBlock(putBlk, blockStoreAddr, &succ)
				check(err)
				if !succ {
					log.Fatal("fail to put block when local has file not in remote1")
				}
			}
			// update file meta
			var latestVersion int32
			err = client.UpdateFile(&FileMetaData{Filename: filename, Version: 1, BlockHashList: localHashes},
				&latestVersion)
			check(err)
			if latestVersion == -1 { // fail -> download from blockstore (others make it first)
				var latestRmMeta map[string]*FileMetaData
				err = client.GetFileInfoMap(&latestRmMeta)
				check(err)
				var bytes []byte
				thisFileMeta := latestRmMeta[filename]
				for _, s := range thisFileMeta.BlockHashList {
					getBlk := &Block{}
					err := client.GetBlock(s, blockStoreAddr, getBlk)
					check(err)
					bytes = append(bytes, getBlk.BlockData...)
				}
				err := os.Remove(ConcatPath(client.BaseDir, filename))
				check(err)
				f, err := os.Create(ConcatPath(client.BaseDir, filename))
				check(err)
				defer f.Close()
				f.Write(bytes)
				localMeta[filename] = thisFileMeta
			} else { // success -> update local index
				localMeta[filename] = &FileMetaData{Filename: filename, Version: latestVersion, BlockHashList: localHashes}
			}
		} else {
			// check version
			remoteFileVersion := remoteIndex[filename].GetVersion()
			remoteHashes := remoteIndex[filename].GetBlockHashList()
			// remote version higher: download, update local index
			if remoteFileVersion >= 1 {
				// delete file case
				if len(remoteHashes) == 1 && remoteHashes[0] == "0" {
					err := os.Remove(ConcatPath(client.BaseDir, filename))
					check(err)
				} else { // normal case
					var bytes []byte
					for _, s := range remoteHashes {
						getBlk := &Block{}
						err := client.GetBlock(s, blockStoreAddr, getBlk)
						check(err)
						bytes = append(bytes, getBlk.BlockData...)
					}
					err := os.Remove(ConcatPath(client.BaseDir, filename))
					check(err)
					f, err := os.Create(ConcatPath(client.BaseDir, filename))
					check(err)
					defer f.Close()
					_, err = f.Write(bytes)
					check(err)
					localMeta[filename] = remoteIndex[filename]
				}
			}
		}
	}
	for filename, localHashes := range updatedLocalFile {
		bytes, err := ioutil.ReadFile(ConcatPath(client.BaseDir, filename))
		check(err)
		for i := range localHashes {
			putBlk := &Block{}
			var succ bool
			if i == len(localHashes)-1 {
				putBlk.BlockData = bytes[i*blockSize:]
				putBlk.BlockSize = int32(len(bytes) - i*blockSize)
			} else {
				putBlk.BlockData = bytes[i*blockSize : (i+1)*blockSize]
				putBlk.BlockSize = int32(blockSize)
			}
			err := client.PutBlock(putBlk, blockStoreAddr, &succ)
			check(err)
			if !succ {
				log.Fatal("fail to put block when local has file not in remote2")
			}
		}
		// update file meta
		var latestVersion int32
		err = client.UpdateFile(&FileMetaData{Filename: filename, Version: localMeta[filename].GetVersion(), BlockHashList: localHashes},
			&latestVersion)
		check(err)
		if latestVersion == -1 { // fail -> download from blockstore (others make it first)
			var latestRmMeta map[string]*FileMetaData
			err = client.GetFileInfoMap(&latestRmMeta)
			check(err)
			var bytes []byte
			thisFileMeta := latestRmMeta[filename]
			for _, s := range thisFileMeta.BlockHashList {
				getBlk := &Block{}
				err := client.GetBlock(s, blockStoreAddr, getBlk)
				check(err)
				bytes = append(bytes, getBlk.BlockData...)
			}
			err := os.Remove(ConcatPath(client.BaseDir, filename))
			check(err)
			f, err := os.Create(ConcatPath(client.BaseDir, filename))
			check(err)
			defer f.Close()
			f.Write(bytes)
			localMeta[filename] = thisFileMeta
		} // else : success -> update local index
	}

	// 1. clean up index.txt
	// 2. update index.txt
	if err := os.Truncate(indexPath, 0); err != nil {
		log.Printf("Failed to truncate: %v", err)
	}
	WriteMetaFile(localMeta, client.BaseDir)
	/*
		fmt.Println("local meta map ")
		PrintMetaMap(localMeta)

		var debugRemote map[string]*FileMetaData
		err = client.GetFileInfoMap(&debugRemote)
		fmt.Println("remote meta map ")
		PrintMetaMap(debugRemote)
	*/
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func FileExists(filename string) bool {
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false
	}
	return !info.IsDir()
}

func testEqHashes(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func stringExist(a string, b []string) bool {
	for _, s := range b {
		if a == s {
			return true
		}
	}
	return false
}
