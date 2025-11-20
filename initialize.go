package adapter_library

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"
)

const (
	defaultLogLevel                = "info"
	defaultPlatformURL             = "http://localhost:9000"
	defaultMessagingURL            = "localhost:1883"
	defaultAdapterConfigCollection = "adapter_config"
	defaultFatalOnDisconnect       = "false"
)

var (
	Args adapterArgs
)

type adapterArgs struct {
	LogLevel                string
	SystemKey               string
	DeviceName              string
	EdgeName                string
	ActiveKey               string
	PlatformURL             string
	MessagingURL            string
	AdapterConfigCollection string
	ServiceAccount          string
	ServiceAccountToken     string
	FatalOnDisconnect       string
}

func ParseArguments(adapterName string) error {
	loggingInit()

	Args = adapterArgs{
		LogLevel:                defaultLogLevel,
		SystemKey:               "",
		DeviceName:              adapterName,
		EdgeName:                "",
		ActiveKey:               "",
		PlatformURL:             defaultPlatformURL,
		MessagingURL:            defaultMessagingURL,
		AdapterConfigCollection: defaultAdapterConfigCollection,
		ServiceAccount:          "",
		ServiceAccountToken:     "",
		FatalOnDisconnect:       defaultFatalOnDisconnect,
	}

	flag.StringVar(&Args.SystemKey, "systemKey", "", "system key (required)")
	flag.StringVar(&Args.DeviceName, "deviceName", adapterName, "name of device (optional)")
	flag.StringVar(&Args.ActiveKey, "password", "", "password (or active key) for device authentication (required)")
	flag.StringVar(&Args.PlatformURL, "platformURL", defaultPlatformURL, "platform url (optional)")
	flag.StringVar(&Args.MessagingURL, "messagingURL", defaultMessagingURL, "messaging URL (optional)")
	flag.StringVar(&Args.LogLevel, "logLevel", defaultLogLevel, "The level of logging to use. Available levels are 'debug, 'info', 'warn', 'error', 'fatal' (optional)")
	flag.StringVar(&Args.AdapterConfigCollection, "adapterConfigCollection", defaultAdapterConfigCollection, "The name of the data collection used to house adapter configuration (optional)")
	flag.StringVar(&Args.FatalOnDisconnect, "fatalOnDisconnect", defaultFatalOnDisconnect, "Exit the application on MQTT connection lost. 'true' or 'false' (optional)")
	flag.Parse()

	setLoggingLevel(Args.LogLevel)

	// check for any values provided by environment variables
	sysKey, ok := os.LookupEnv("CB_SYSTEM_KEY")
	if ok && Args.SystemKey == "" {
		Args.SystemKey = sysKey
		log.Println("[DEBUG] Using ClearBlade System Key From Environment Variable")
	}
	servAcc, ok := os.LookupEnv("CB_SERVICE_ACCOUNT")
	if ok {
		Args.ServiceAccount = servAcc
		log.Println("[DEBUG] Using provided Service Account for Device Name")
	}
	token, ok := os.LookupEnv("CB_SERVICE_ACCOUNT_TOKEN")
	if ok {
		Args.ServiceAccountToken = token
	}
	edgeName, ok := os.LookupEnv("CB_EDGE_NAME")
	if ok && Args.EdgeName == "" {
		Args.EdgeName = edgeName
	}

	// verify all required fields are present
	if Args.SystemKey == "" {
		return fmt.Errorf("System Key is required, can be supplied with --systemKey flag or CB_SYSTEM_KEY environment variable")
	}
	if Args.ActiveKey == "" && Args.ServiceAccount == "" {
		return fmt.Errorf("Device Password is required when not using a Service Account, can be supplied with --password flag")
	}
	if Args.ServiceAccount != "" && Args.ServiceAccountToken == "" {
		return fmt.Errorf("Service Account Token is required when a Service Account is specified, this should have automatically been supplied. Check for typos then try again")
	}

	log.Printf("[DEBUG] ParseArguments - Final arguments being used: %+v\n", Args)
	return nil
}

func Initialize() (*AdapterConfig, error) {
	log.Println("[INFO] Initialize - Adapter Library running needed inits")

	rand.Seed(time.Now().UnixNano())

	// auth with edge/platform
	if Args.ServiceAccount != "" {
		err := authWithServiceAccount()
		if err != nil {
			return nil, err
		}
	} else {
		err := authWithDevice()
		if err != nil {
			return nil, err
		}
	}

	// fetch adapter config
	adapterConfig, err := FetchAdapterConfig()
	if err != nil {
		return nil, err
	}

	return adapterConfig, nil
}
