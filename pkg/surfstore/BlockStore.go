package surfstore

import (
	context "context"
	"sync"
)

type BlockStore struct {
	BlockMap map[string]*Block
	mtx      sync.Mutex
	UnimplementedBlockStoreServer
}

func (bs *BlockStore) GetBlock(ctx context.Context, blockHash *BlockHash) (*Block, error) {
	//panic("todo")
	bs.mtx.Lock()
	defer bs.mtx.Unlock()
	hashVal := blockHash.GetHash()
	data := bs.BlockMap[hashVal].GetBlockData()
	size := bs.BlockMap[hashVal].GetBlockSize()
	/*
		log.Println("server get1:", len(data), " ", data[:10])
		log.Println("server get2:", size)
	*/
	return &Block{
		BlockData: data,
		BlockSize: size,
	}, nil
}

func (bs *BlockStore) PutBlock(ctx context.Context, block *Block) (*Success, error) {
	//panic("todo")
	bs.mtx.Lock()
	defer bs.mtx.Unlock()
	blockData := block.GetBlockData()
	blockSize := block.GetBlockSize()
	blockPrepared := &Block{
		BlockData: blockData,
		BlockSize: blockSize,
	}
	hashString := GetBlockHashString(blockData)
	bs.BlockMap[hashString] = blockPrepared
	return &Success{Flag: true}, nil
}

// Given a list of hashes “in”, returns a list containing the
// subset of in that are stored in the key-value store
func (bs *BlockStore) HasBlocks(ctx context.Context, blockHashesIn *BlockHashes) (*BlockHashes, error) {
	//panic("todo")
	bs.mtx.Lock()
	defer bs.mtx.Unlock()
	hashes := blockHashesIn.GetHashes()
	var exist []string
	for _, s := range hashes {
		if _, ok := bs.BlockMap[s]; ok {
			//hashString := GetBlockHashString(block.GetBlockData())
			exist = append(exist, s)
		}
	}
	return &BlockHashes{Hashes: exist}, nil
}

// This line guarantees all method for BlockStore are implemented
var _ BlockStoreInterface = new(BlockStore)

func NewBlockStore() *BlockStore {
	return &BlockStore{
		BlockMap: map[string]*Block{},
	}
}
