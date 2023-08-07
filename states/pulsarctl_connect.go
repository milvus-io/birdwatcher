package states

import (
	"context"
	"fmt"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/birdwatcher/framework"
	pulsarctl "github.com/streamnative/pulsarctl/pkg/pulsar"
	"github.com/streamnative/pulsarctl/pkg/pulsar/common"
	"github.com/streamnative/pulsarctl/pkg/pulsar/utils"
)

type PulsarctlParam struct {
	framework.ParamBase `use:"pulsarctl" desc:"connect to pulsar admin with pulsarctl"`
	Address             string `name:"addr" default:"http://localhost:18080" desc:"pulsar admin address"`
	AuthPlugin          string `name:"authPlugin" default:"" desc:"pulsar admin auth plugin"`
	AuthParam           string `name:"authParam" default:"" desc:"pulsar admin auth parameters"`
}

func (app *ApplicationState) PulsarctlCommand(ctx context.Context, p *PulsarctlParam) error {

	config := common.Config{
		WebServiceURL:    p.Address,
		AuthPlugin:       p.AuthPlugin,
		AuthParams:       p.AuthParam,
		PulsarAPIVersion: common.V2,
	}
	admin, err := pulsarctl.New(&config)
	if err != nil {
		fmt.Println("failed to build pulsar admin client, error:", err.Error())
		return err
	}

	adminState := getPulsarAdminState(app.core, admin, p.Address)
	app.SetTagNext(pulsarTag, adminState)
	return nil
}

func getPulsarAdminState(parent *framework.CmdState, admin pulsarctl.Client, addr string) framework.State {
	state := &pulsarAdminState{
		CmdState: parent.Spawn(fmt.Sprintf("PulsarAdmin(%s)", addr)),
		admin:    admin,
		addr:     addr,
	}
	return state
}

type pulsarAdminState struct {
	*framework.CmdState
	admin pulsarctl.Client
	addr  string

	topics []string
}

// SetupCommands setups the command.
// also called after each command run to reset flag values.
func (s *pulsarAdminState) SetupCommands() {
	cmd := s.GetCmd()

	s.UpdateState(cmd, s, s.SetupCommands)
}

type ListTopicParam struct {
	framework.ParamBase `use:"list topic" desc:"list topics in instance"`
	Tenant              string `name:"tenant" default:"public" desc:"tenant name to list"`
	Namespace           string `name:"namespace" default:"default" desc:"namespace name to list"`
}

func (s *pulsarAdminState) ListTopicCommand(ctx context.Context, p *ListTopicParam) error {
	nsn, err := utils.GetNameSpaceName(p.Tenant, p.Namespace)
	if err != nil {
		return err
	}
	npt, pt, err := s.admin.Topics().List(*nsn)
	if err != nil {
		return errors.Wrap(err, "failed to list topics")
	}
	fmt.Println("Non-persist topics:")
	for _, topic := range npt {
		fmt.Println(topic)
	}
	fmt.Println("Persist topics:")
	for _, topic := range pt {
		fmt.Println(topic)
	}
	return nil
}

type ListSubscriptionParam struct {
	framework.ParamBase `use:"list subscription" desc:"list subscriptions for provided topic"`
	Tenant              string `name:"tenant" default:"public" desc:"tenant name to list"`
	Namespace           string `name:"namespace" default:"default" desc:"namespace name to list"`
	Topic               string `name:"topic" default:"" desc:"topic to check subscription"`
}

func (s *pulsarAdminState) ListSubscriptionCommand(ctx context.Context, p *ListSubscriptionParam) error {

	topicName, err := utils.GetTopicName(fmt.Sprintf("%s/%s/%s", p.Tenant, p.Namespace, p.Topic))
	if err != nil {
		return errors.Wrap(err, "failed to parse topic name")
	}

	subscriptions, err := s.admin.Subscriptions().List(*topicName)
	if err != nil {
		return errors.Wrap(err, "failed to list subscriptions")
	}
	for _, subName := range subscriptions {
		fmt.Println(subName)
	}
	return nil
}
