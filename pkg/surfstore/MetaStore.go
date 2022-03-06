package surfstore

import (
	context "context"
	"fmt"
	"sync"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type MetaStore struct {
	FileMetaMap    map[string]*FileMetaData
	BlockStoreAddr string
	mtx            sync.Mutex
	UnimplementedMetaStoreServer
}

func (m *MetaStore) GetFileInfoMap(ctx context.Context, _ *emptypb.Empty) (*FileInfoMap, error) {
	//panic("todo")
	m.mtx.Lock()
	defer m.mtx.Unlock()
	return &FileInfoMap{
		FileInfoMap: m.FileMetaMap,
	}, nil
}

func (m *MetaStore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) {
	//panic("todo")
	m.mtx.Lock()
	defer m.mtx.Unlock()
	fileName := fileMetaData.GetFilename()
	fileVersion := fileMetaData.GetVersion()
	// file doesn't exist
	if _, ok := m.FileMetaMap[fileName]; !ok {
		if fileVersion != 1 { // pre: version is 1
			return &Version{Version: -1}, fmt.Errorf("new file but version is not 1")
		}
		m.FileMetaMap[fileName] = fileMetaData
		return &Version{Version: 1}, nil
	} else { // file exist
		inMetaData := m.FileMetaMap[fileName]
		hashList := inMetaData.GetBlockHashList()
		// check tombstone case || previously deleted
		if len(hashList) == 1 && hashList[0] == "0" {
			vv := inMetaData.Version + 1 // increment by 1
			m.FileMetaMap[fileName] = fileMetaData
			m.FileMetaMap[fileName].Version = vv
			return &Version{Version: vv}, nil
		}
		// normal update
		if inMetaData.Version+1 == fileMetaData.GetVersion() {
			m.FileMetaMap[fileName] = fileMetaData
			return &Version{Version: fileMetaData.GetVersion()}, nil
		}
		return &Version{Version: -1}, nil
	}
}

func (m *MetaStore) GetBlockStoreAddr(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddr, error) {
	//panic("todo")
	m.mtx.Lock()
	defer m.mtx.Unlock()
	return &BlockStoreAddr{Addr: m.BlockStoreAddr}, nil
}

// This line guarantees all method for MetaStore are implemented
var _ MetaStoreInterface = new(MetaStore)

func NewMetaStore(blockStoreAddr string) *MetaStore {
	return &MetaStore{
		FileMetaMap:    map[string]*FileMetaData{},
		BlockStoreAddr: blockStoreAddr,
	}
}
