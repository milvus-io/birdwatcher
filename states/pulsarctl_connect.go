package states

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
	pulsarctl "github.com/streamnative/pulsarctl/pkg/pulsar"
	pulsarcommon "github.com/streamnative/pulsarctl/pkg/pulsar/common"
	"github.com/streamnative/pulsarctl/pkg/pulsar/utils"

	"github.com/milvus-io/birdwatcher/common"
	"github.com/milvus-io/birdwatcher/framework"
)

type PulsarctlParam struct {
	framework.ParamBase `use:"pulsarctl" desc:"connect to pulsar admin with pulsarctl"`
	Address             string `name:"addr" default:"http://localhost:18080" desc:"pulsar admin address"`
	AuthPlugin          string `name:"authPlugin" default:"" desc:"pulsar admin auth plugin"`
	AuthParam           string `name:"authParam" default:"" desc:"pulsar admin auth parameters"`
}

func (s *disconnectState) PulsarctlCommand(ctx context.Context, p *PulsarctlParam) error {
	config := pulsarcommon.Config{
		WebServiceURL:    p.Address,
		AuthPlugin:       p.AuthPlugin,
		AuthParams:       p.AuthParam,
		PulsarAPIVersion: pulsarcommon.V2,
	}
	admin, err := pulsarctl.New(&config)
	if err != nil {
		fmt.Println("failed to build pulsar admin client, error:", err.Error())
		return err
	}

	adminState := getPulsarAdminState(admin, p.Address)
	s.SetNext(adminState)
	return nil
}

func getPulsarctlCmd(state common.State) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "pulsarctl",
		Short: "connect to pulsar admin with pulsarctl",
		Run: func(cmd *cobra.Command, args []string) {
			address, err := cmd.Flags().GetString("addr")
			if err != nil {
				fmt.Println(err.Error())
			}
			authPlugin, err := cmd.Flags().GetString("authPlugin")
			if err != nil {
				fmt.Println(err.Error())
			}
			authParams, err := cmd.Flags().GetString("authParams")
			if err != nil {
				fmt.Println(err.Error())
			}

			config := pulsarcommon.Config{
				WebServiceURL:    address,
				AuthPlugin:       authPlugin,
				AuthParams:       authParams,
				PulsarAPIVersion: pulsarcommon.V2,
			}
			admin, err := pulsarctl.New(&config)
			if err != nil {
				fmt.Println("failed to build pulsar admin client, error:", err.Error())
			}

			adminState := getPulsarAdminState(admin, address)
			state.SetNext(adminState)
		},
	}

	cmd.Flags().String("addr", "http://localhost:18080", "pulsar admin address")
	cmd.Flags().String("authPlugin", "", "pulsar admin auth plugin")
	cmd.Flags().String("authParams", "", "pulsar admin auth parameters")

	return cmd
}

func getPulsarAdminState(admin pulsarctl.Client, addr string) common.State {
	state := &pulsarAdminState{
		CmdState: common.CmdState{
			LabelStr: fmt.Sprintf("PulsarAdmin(%s)", addr),
		},
		admin: admin,
		addr:  addr,
	}
	state.SetupCommands()
	return state
}

type pulsarAdminState struct {
	common.CmdState
	admin pulsarctl.Client
	addr  string

	topics []string
}

// SetupCommands setups the command.
// also called after each command run to reset flag values.
func (s *pulsarAdminState) SetupCommands() {
	cmd := &cobra.Command{}

	cmd.AddCommand(

		getListTopicCmd(s.admin),

		getListSubscriptionCmd(s.admin),

		getExitCmd(s),
	)

	s.CmdState.RootCmd = cmd
	s.SetupFn = s.SetupCommands
}

func getListTopicCmd(admin pulsarctl.Client) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "list-topic",
		Short: "list topics in instance",
		Run: func(cmd *cobra.Command, args []string) {
			nsn, err := utils.GetNameSpaceName("public", "default")
			if err != nil {
				fmt.Println(err.Error())
				return
			}
			np, p, err := admin.Topics().List(*nsn)
			if err != nil {
				fmt.Println("failed to list topics", err.Error())
				return
			}
			fmt.Println("Non-persist topics:")
			for _, topic := range np {
				fmt.Println(topic)
			}
			fmt.Println("Persist topics:")
			for _, topic := range p {
				fmt.Println(topic)
			}
		},
	}

	return cmd
}

func getListSubscriptionCmd(admin pulsarctl.Client) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "list-subscription",
		Short: "list topic subscriptions",
		Run: func(cmd *cobra.Command, args []string) {
			topic, err := cmd.Flags().GetString("topic")
			if err != nil {
				fmt.Println(err.Error())
				return
			}

			topicName, err := utils.GetTopicName(topic)
			if err != nil {
				fmt.Println("failed to parse topic name", err.Error())
				return
			}

			subscriptions, err := admin.Subscriptions().List(*topicName)
			if err != nil {
				fmt.Println("failed to list subscriptions", err.Error())
				return
			}
			for _, subName := range subscriptions {
				fmt.Println(subName)
			}
		},
	}

	cmd.Flags().String("topic", "", "target topic to list")
	return cmd
}
