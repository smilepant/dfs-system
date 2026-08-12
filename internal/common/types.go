package common

type StorageNodeInfo struct {
	NodeID     string `json:"node_id"`
	Address    string `json:"address"` // "host:port"
	TotalSpace int64  `json:"total_space"`
	FreeSpace  int64  `json:"free_space"`
}

type NodeRegisterRequest struct {
	NodeID     string `json:"node_id" binding:"required"`
	Address    string `json:"address" binding:"required"`
	TotalSpace int64  `json:"total_space"`
	FreeSpace  int64  `json:"free_space"`
}

type NodeRegisterResponse struct {
	Status  string `json:"status"`
	Message string `json:"message"`
}
