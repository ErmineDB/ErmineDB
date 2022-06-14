package pubsub

import (
	"log"
	//     "sync"
    "time"
    "github.com/shamaton/msgpack/v2"
	"strconv"
	"github.com/ErmineDB/ErmineDB/internal/formatter"
	"github.com/ErmineDB/ErmineDB/internal/helpers"
    "github.com/ErmineDB/ErmineDB/cmd/ermined/protocol"
//     "github.com/ErmineDB/ErmineDB/cmd/ermined/eRaft"
)

var Channels []helpers.Channel

type dataStore struct {
	Data    string
}

func Commands() []string {
	commands := []string{"Publish", "Subscribe", "Unsubscribe"}
	return commands
}

// type ProtoHandler struct {
// 	s *eRaft.Server
// }

func NewWatcher(addrRedisportRaftPort string, hdl protocol.ProtoHandler) {
    dataObj := &dataStore{
        Data: ""}

    do, err := msgpack.Marshal(&dataObj)
	if err != nil {
		panic(err)
	}
	hdl.S.RaftSet("bucket0test", do)
    time.Sleep(1000 * time.Millisecond)

    x := 1
    for x > 0 {
        resultObj, _ := hdl.S.Store.GetData([]byte("bucket0test"))
        resLen := len(resultObj)
        if resLen == 0 {
            log.Printf("No Data")
        } else {
            var m dataStore
            err := msgpack.Unmarshal(resultObj, &m)
            if err != nil {
                panic(err)
            }

            if m.Data != "" {
                log.Printf("I should publish this: %s", m.Data)
                // msgpack
                m.Data = ""
                do, err := msgpack.Marshal(&m)
                if err != nil {
                    panic(err)
                }
                hdl.S.RaftSet("bucket0test", do)
            }
        }
        time.Sleep(1000 * time.Millisecond)
    }
}

func Call(funcName string, client helpers.Client, params ...interface{}) {
	//     var results string

	switch funcName {
	case "Subscribe":
		Subscribe(params[0].([]string), client)
	case "Publish":
		Publish(params[0].([]string), client)
	}

	return
}

func ChannelContains(str string) (int, bool) {
	i := 0
	for _, v := range Channels {
		if v.Name == str {
			return i, true
		}
		i += 1
	}
	return -1, false
}

// create a new channel and add it to Channels.
// search helpers.Manager[].Client.subs and see if any clients should be subscribed
func addChannel(name string) {
	newChannel := helpers.Channel{}
	newChannel.Name = name
	//     newChannel.Clients = append(newChannel.Clients, client)
	Channels = append(Channels, newChannel)
}

func RemoveFromChannel(client helpers.Client, channelName string) bool {
	for _, c := range Channels {
		if c.Name == channelName {
			i := 0
			for cl := range c.Clients {
				if c.Clients[cl].SessionId == client.SessionId {
					c.Clients[i] = c.Clients[len(c.Clients)-1]
					c.Clients = c.Clients[:len(c.Clients)-1]
					return true
				}
				i += 1
			}
		}
	}
	return false
}

// func RemoveFromChannels(client helpers.Client) {
//     for _, c := range Channels {
//         i := 0
//         for cl := range c.Clients {
//             if cl.Uuid == client.Uuid {
//                 c.Clients[i] = c.Clients[len(c.Clients)-1]
//                 c.Clients = c.Clients[:len(c.Clients)-1]
//             }
//             i += 1
//         }
//     }
// }

// add sub to Channels.clients.  If the channel doesn't exist, create a
// new channel and add it to Channels first.
func Subscribe(data []string, client helpers.Client) {
	//     log.Printf("PubSub Subscribe data: %v", data)

	argobj := formatter.ProcArgs(data)

	//     idx := 1
	for _, k := range argobj.Args {

		if i, ok := ChannelContains(k); ok {
			//             log.Printf("found channel %v", k)

			// update client with new sub, then update the Manager with client
			client.Subs = append(client.Subs, k)
			helpers.Manager.Store(client.SessionId, client)

			// load existing channel and add client
			// then update Channels
			existingChannel := Channels[i]
			existingChannel.Clients = append(existingChannel.Clients, client)
			Channels[i] = existingChannel

		} else {
			//             log.Printf("adding channel %v", k)
			// create a new channel
			newChannel := helpers.Channel{}
			newChannel.Name = k

			// update client with new sub, then update the Manager with client
			client.Subs = append(client.Subs, k)
			helpers.Manager.Store(client.SessionId, client)

			newChannel.Clients = append(newChannel.Clients, client)

			// add the newChannel to Channels
			Channels = append(Channels, newChannel)
		}

		// setup data to be returned to the client
		// number of objs in array, command used, channel, number of channels the client is subscribed
		resData := "*3" + "\r\n"
		resData += "$" + strconv.Itoa(len([]rune(argobj.Command))) + "\r\n" + argobj.Command + "\r\n"
		resData += "$" + strconv.Itoa(len([]rune(k))) + "\r\n" + k + "\r\n"
		resData += ":" + strconv.FormatInt(int64(len(client.Subs)), 10) + "\r\n"

		client.Socket.Write([]byte(resData))

	}
}

func Publish(data []string, client helpers.Client) {
	argobj := formatter.ProcArgs(data)

	// we need to count how many clients we published messages to
	count := 0
	if i, ok := ChannelContains(argobj.Args[0]); ok {
		// if the channel exists, write the message to all the clients
		log.Printf("found channel %v", argobj.Args[0])
		var err error

		// try to write message to all clients subscribed to channel
		resData := "*3" + "\r\n"
		resData += "$" + strconv.Itoa(len([]rune("message"))) + "\r\n" + "message" + "\r\n"
		resData += "$" + strconv.Itoa(len([]rune(argobj.Args[0]))) + "\r\n" + argobj.Args[0] + "\r\n"
		resData += "$" + strconv.Itoa(len([]rune(argobj.Args[1]))) + "\r\n" + argobj.Args[1] + "\r\n"
		for _, c := range Channels[i].Clients {
			_, err = c.Socket.Write([]byte(resData))
			if err != nil {
				RemoveFromChannel(c, argobj.Args[0])
			} else {
				count += 1
			}

		}
	} else {
		// if the channel doesn't exist, just create the channel and add it to Channels.
		newChannel := helpers.Channel{}
		newChannel.Name = argobj.Args[0]
		Channels = append(Channels, newChannel)
	}
	client.Socket.Write([]byte(formatter.Integer(int64(count))))
}
