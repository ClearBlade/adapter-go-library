# adapter-go-library
The adapter-go-library streamlines the development process for ClearBlade Adapters. Most of the interaction with the ClearBlade Edge/Platform has been abstracted out, including Authentication, Adapter Configuration Collection, and MQTT Connection.

## Usage
```golang
package main

import (
	"log"

	adapter_library "github.com/clearblade/adapter-go-library"
	mqttTypes "github.com/clearblade/mqtt_parsing"
)

const (
	adapterName = "my-new-adapter"
)

var (
	adapterConfig    *adapter_library.AdapterConfig
)

func main() {

	// add any adapter specific command line flags needed here, before calling ParseArguments

	err := adapter_library.ParseArguments(adapterName)
	if err != nil {
		log.Fatalf("[FATAL] Failed to parse arguments: %s\n", err.Error())
	}

	// initialize all things ClearBlade, includes authenticating if needed, and fetching the
	// relevant adapter_config collection entry
	adapterConfig, err = adapter_library.Initialize()
	if err != nil {
		log.Fatalf("[FATAL] Failed to initialize: %s\n", err.Error())
	}
	
	// if your adapter config includes custom adapter settings, parse/validate them here
	
	// connect MQTT, if your adapter needs to subscribe to a topic, provide it as the first
	// parameter, and a callback for when messages are received. if no need to subscribe,
	// simply provide an empty string and nil
	// ConnectMQTT will block until MQTT connection is successfully created
	err = adapter_library.ConnectMQTT(adapterConfig.TopicRoot+"/outgoing/#", cbMessageHandler)
	if err != nil {
		log.Fatalf("[FATAL] Failed to Connect MQTT: %s\n", err.Error())
	}
	
	// kick off adapter specific things here

	// keep adapter executing indefinitely 
	select {}
}

func cbMessageHandler(message *mqttTypes.Publish) {
	// process incoming MQTT messages as needed here
}
```

## Command Line Arguments & Environment Variables
All ClearBlade Adapters require a certain set of System specific variables to start and connect with the ClearBlade Platform/Edge. This library allows these to be passed in either by command line arguments, or environment variables. Note that command line arguments take precedence over environment variables.

| Name | CLI Flag | Environment Variable | Default |
| --- | --- | --- | --- |
| System Key | `systemKey` | `CB_SYSTEM_KEY` | N/A |
| Platform/Edge URL | `platformURL` | N/A | `http://localhost:9000` |
| Platform/Edge Messaging URL | `messagingURL` | N/A | `localhost:1883` |
| Device Service Account | N/A | `CB_SERVICE_ACCOUNT` | N/A |
| Device Service Account Token | N/A | `CB_SERVICE_ACCOUNT_TOKEN` | N/A |
| Log Level | `logLevel` | N/A | `info` |
| Adapter Config Collection Name | `adapterConfigCollection` | N/A | `adapter_config` |

A System Key will always be required to start the adapter, and it's recommended to always use a Device Service Account & Token for Adapters. 

## Adapter Configuration & Settings
This library does require the use of an Adapter Configuration Collection. This allows you to easily provide configuration options to your adapter at runtime via this Collection, rather than command line arguments, environment variables, or hardcoded values in your adapter. 

If your adapter does not have any specific settings it is still expected to have an entry in this collection, but adapter settings column can be left blank.

The default name for this Collection is `adapter_config`, and it's expected data structure is:

| Column Name | Column Type |
| --- | --- |
| `adapter_name` | `string` |
| `topic_root` | `string` |
| `adapter_settings` | `string` |

## Logging
This adapter introduces some basic logging level that your adapter can also leverage. Simply preface any of your log statements with the relevant level of the log surrounded with square brackets, and the `logLevel` flag passed in will be honored by your specific adapter logs. 

The supported log levels are `DEBUG`, `INFO`, `ERROR`, and `FATAL`.

## Fatal Errors
There are a few cases where this library will encounter a fatal error and cause your adapter to quit. A log with more information will be provided (e.g. If your adapter subscribes to a topic and the device account used by the adapter does not have subscribe permissions on this topic)

