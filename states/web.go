package states

import (
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/spf13/cobra"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/birdwatcher/states/kv"
)

// getCmdCmd returns exit command for input state.
func getWebCmd(state framework.State, cli kv.MetaKV, basePath string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "web",
		Short: "start a web server to see more details on browser",
		RunE: func(cmd *cobra.Command, args []string) error {
			port, err := cmd.Flags().GetInt("port")
			if err != nil {
				fmt.Println(err.Error())
				return err
			}
			bindAddr := fmt.Sprintf("0.0.0.0:%d", port)
			server := &http.Server{Addr: bindAddr, ReadTimeout: 10 * time.Second}
			http.Handle("/", ginHandler(cli, basePath))
			if err = server.ListenAndServe(); err != nil {
				fmt.Println(err.Error())
				return err
			}
			return nil
		},
	}
	cmd.Flags().Int("port", 8080, "port to listen")
	return cmd
}

func ginHandler(cli kv.MetaKV, basePath string) http.Handler {
	r := gin.Default()
	r.GET("/ping", func(c *gin.Context) {
		c.String(http.StatusOK, "pong")
	})
	r.GET("/collections", func(c *gin.Context) {
		collections, err := common.ListCollections(c, cli, basePath)
		if err != nil {
			c.Error(err)
			return
		}
		c.JSON(http.StatusOK, collections)
	})
	return r
}
