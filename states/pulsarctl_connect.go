package states

import (
	"fmt"

	"github.com/spf13/cobra"
	pulsarctl "github.com/streamnative/pulsarctl/pkg/pulsar"
	"github.com/streamnative/pulsarctl/pkg/pulsar/common"
	"github.com/streamnative/pulsarctl/pkg/pulsar/utils"
)

func getPulsarctlCmd(state State) *cobra.Command {
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

			config := common.Config{
				WebServiceURL:    address,
				AuthPlugin:       authPlugin,
				AuthParams:       authParams,
				PulsarAPIVersion: common.V2,
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

func getPulsarAdminState(admin pulsarctl.Client, addr string) State {
	state := &pulsarAdminState{
		cmdState: cmdState{
			label: fmt.Sprintf("PulsarAdmin(%s)", addr),
		},
		admin: admin,
		addr:  addr,
	}
	state.SetupCommands()
	return state
}

type pulsarAdminState struct {
	cmdState
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

	s.cmdState.rootCmd = cmd
	s.setupFn = s.SetupCommands
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
