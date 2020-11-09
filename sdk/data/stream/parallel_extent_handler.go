package stream

import (
	"fmt"
	"github.com/chubaofs/chubaofs/sdk/data/wrapper"
	"github.com/gogo/protobuf/proto"
	"net"
)

type ParallelExtentHandler struct {
	dp *wrapper.DataPartition
	requests chan *Packet
	connects  []net.Conn
}

func NewParallelExtentHandler(dp *wrapper.DataPartition,leaderConn net.Conn) (handler *ParallelExtentHandler,err error){
	handler=new(ParallelExtentHandler)
	handler.dp=dp
	handler.requests=make(chan *Packet,1024)
	handler.connects=make([]net.Conn,dp.ReplicaNum)
	handler.connects[0]=leaderConn
	for index:=1;index<len(dp.Hosts);index++{
		handler.connects[index],err=StreamConnPool.GetConnect(dp.Hosts[index])
		if err!=nil {
			return nil,fmt.Errorf("cannot get connect from (%v) error(%v)",dp.Hosts[index],err)
		}
	}
	return handler,nil
}


func (handler *ParallelExtentHandler)write(p *Packet) (err error){
	for _,conn:=range handler.connects{
		if err=p.writeToConn(conn);err!=nil {
			return fmt.Errorf("write to (%v) error(%v)",conn.RemoteAddr().String(),err)
		}
	}
}

func (handler *ParallelExtentHandler)readResponse(reply *Packet){
	for _,conn:=range handler.connects{
		reply.ResultCode=proto.DiscardUnknown()
		reply.readFromConn()
	}
}


