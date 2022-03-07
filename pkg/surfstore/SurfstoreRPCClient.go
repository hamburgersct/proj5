package surfstore

import (
	context "context"
	"errors"
	"time"

	grpc "google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

type RPCClient struct {
	MetaStoreAddrs []string
	BaseDir        string
	BlockSize      int
}

// BlockStore
func (surfClient *RPCClient) GetBlock(blockHash string, blockStoreAddr string, block *Block) error {
	// connect to the server

	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)
	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	b, err := c.GetBlock(ctx, &BlockHash{Hash: blockHash})
	if err != nil {
		conn.Close()
		return err
	}
	block.BlockData = b.BlockData
	block.BlockSize = b.BlockSize
	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) PutBlock(block *Block, blockStoreAddr string, succ *bool) error {

	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)
	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	success, err := c.PutBlock(ctx, &Block{BlockData: block.BlockData, BlockSize: block.BlockSize})

	if err != nil {
		conn.Close()
		return err
	}
	*succ = success.Flag
	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) HasBlocks(blockHashesIn []string, blockStoreAddr string, blockHashesOut *[]string) error {
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)
	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	hashOut, err := c.HasBlocks(ctx, &BlockHashes{Hashes: blockHashesIn})
	if err != nil {
		conn.Close()
		return err
	}
	*blockHashesOut = hashOut.Hashes
	// close the connection
	return conn.Close()
}

// MetaStore
func (surfClient *RPCClient) GetFileInfoMap(serverFileInfoMap *map[string]*FileMetaData) error {
	// connect to the server
	for _, addr := range surfClient.MetaStoreAddrs {
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			return err
		}
		c := NewRaftSurfstoreClient(conn)
		// perform the call
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
		defer cancel()
		fInfo, err := c.GetFileInfoMap(ctx, &emptypb.Empty{})
		if errors.Is(err, ERR_NOT_LEADER) { // continue looking for leader
			continue
		}
		if err != nil {
			conn.Close()
			return err

		}
		*serverFileInfoMap = fInfo.FileInfoMap
		// close the connection
		return conn.Close()
	}
	return nil
}

func (surfClient *RPCClient) UpdateFile(fileMetaData *FileMetaData, latestVersion *int32) error {
	// connect to the server
	for _, addr := range surfClient.MetaStoreAddrs {
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			return err
		}
		c := NewRaftSurfstoreClient(conn)
		// perform the call
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
		defer cancel()
		version, err := c.UpdateFile(ctx, fileMetaData)
		if errors.Is(err, ERR_NOT_LEADER) { // continue looking for leader
			continue
		}
		if err != nil {
			conn.Close()
			return err

		}
		*latestVersion = version.Version
		// close the connection
		return conn.Close()
	}
	return nil
}

func (surfClient *RPCClient) GetBlockStoreAddr(blockStoreAddr *string) error {
	// connect to the server

	for _, addr := range surfClient.MetaStoreAddrs {
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			return err
		}
		c := NewRaftSurfstoreClient(conn)
		// perform the call
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
		defer cancel()
		addr, err := c.GetBlockStoreAddr(ctx, &emptypb.Empty{})
		if errors.Is(err, ERR_NOT_LEADER) { // continue looking for leader
			continue
		}
		if err != nil {
			conn.Close()
			return err
		}
		*blockStoreAddr = addr.Addr
		// close the connection
		return conn.Close()
	}
	return nil
}

// This line guarantees all method for RPCClient are implemented
var _ ClientInterface = new(RPCClient)

// Create an Surfstore RPC client
func NewSurfstoreRPCClient(addrs []string, baseDir string, blockSize int) RPCClient {
	return RPCClient{
		MetaStoreAddrs: addrs,
		BaseDir:        baseDir,
		BlockSize:      blockSize,
	}
}
