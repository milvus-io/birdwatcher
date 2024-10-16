# Birdwatcher command framework

Using `AddCommand` from `cobra.Command` for each command is annoying. Birdwatcher implemented its own command definition and detection framework. It could be done by defining two entities for one command.

## Auto detection

All *public* method of `states.State` will be iterated to check whether it is a "Command Function" or not. One method shall be a "Command Function", which means it will be auto registered as a command if:

- The method name is in format of "XXXXXCommand", which must be ended with "Command", case sensitive.
- The method must have two input parameters
    - First parameter shall be `context.Context`
    - Second shall be a pointer of a struct implementing `framework.CmdParam`
- The return type list could be:
    - Just one value, which is in type of `error` standing for whether the execution is successful or not.
    - Two values, the second is in type of `error`, and the first one is in type which implements `framework.ResultSet`

Here is one example

``` go
// states/etcd_connect.go

type ConnectParams struct {
	framework.ParamBase `use:"connect" desc:"Connect to etcd"`
    EtcdAddr            string `name:"etcd" default:"127.0.0.1:2379" desc:"the etcd endpoint to connect"`
    // omit other detail definition
}

func (s *disconnectState) ConnectCommand(ctx context.Context, cp *ConnectParams) error {
    // ...
    return nil
}
```

`ConnectCommand` will be registerer using the golang tag of `framework.ParamBase`, which saying the command shall use `connect`. This is the most common command used in birdwatcher.

And the `etcd` param is alos defined via tagging feature. Framework auto detects the type from the type `string` and set the param name with `name` tag, the default value from `default` tag and the description text from `desc` tag.

## Embed component

If the commands are many, if could be hard to maintain for just one state with many methods. Actually in current implementation, etcd show commands are often defined under `state/etcd` package.

So birdwatcer framework also provided the ability to read anonymous embedded component method to scan command candiates as well.