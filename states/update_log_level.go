package states

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"

	"github.com/spf13/cobra"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
)

// TODO read port from config
const httpAPIListenPort = 9091

func getShowLogLevelCmd(cli clientv3.KV, basePath string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "show-log-level",
		Short: "show log level of milvus roles",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			sessions, err := common.ListSessions(ctx, cli, basePath)
			if err != nil {
				return err
			}

			httpClient := &http.Client{}
			for _, session := range sessions {
				err := GetLogLevel(httpClient, session)
				if err != nil {
					return err
				}
			}
			return nil
		},
	}

	return cmd
}

func GetLogLevel(httpClient *http.Client, session *models.Session) error {
	ip := strings.Split(session.Address, ":")[0]
	url := getLogLevelURL(ip)

	resp, err := httpClient.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	fmt.Printf("%s-%d:\t%s", session.ServerName, session.ServerID, string(body))
	return nil
}

func getUpdateLogLevelCmd(cli clientv3.KV, basePath string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "update-log-level log_level [component] [serverId]",
		Short: "update log level of milvus role ",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			sessions, err := common.ListSessions(ctx, cli, basePath)
			if err != nil {
				return err
			}

			if len(args) < 1 {
				cmd.Usage()
				return nil
			}
			targetLevel := strings.ToLower(args[0])

			targetRole := ""
			if len(args) > 1 {
				targetRole = strings.ToLower(args[1])
			}

			targetServerID := int64(-1)
			if len(args) == 3 {
				serverID, err := strconv.ParseInt(args[2], 10, 64)
				if err != nil {
					return err
				}
				targetServerID = serverID
			}

			httpClient := &http.Client{}
			foundComponent := false
			for _, session := range sessions {
				if targetRole == "" ||
					(targetServerID == -1 && targetRole == session.ServerName) ||
					(targetRole == session.ServerName && targetServerID == session.ServerID) {
					foundComponent = true
					err := changeLogLevel(httpClient, session, targetLevel)
					if err != nil {
						return err
					}
				}
			}

			if !foundComponent {
				fmt.Printf("component=%s, serverId=%d not found!", targetRole, targetServerID)
				return nil
			}

			return nil
		},
	}

	return cmd
}

func changeLogLevel(httpClient *http.Client, session *models.Session, newLevel string) error {
	ip := strings.Split(session.Address, ":")[0]
	logURL := getLogLevelURL(ip)

	payload, err := json.Marshal(map[string]interface{}{"level": newLevel})
	if err != nil {
		return err
	}

	req, err := http.NewRequest(http.MethodPut, logURL, bytes.NewBuffer(payload))
	req.Header.Set("Content-Type", "application/json")
	if err != nil {
		return err
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	fmt.Printf("%s-%d:\t%s", session.ServerName, session.ServerID, string(body))
	return nil
}

func getLogLevelURL(ip string) string {
	return fmt.Sprintf("http://%s:%d/log/level", ip, httpAPIListenPort)
}
