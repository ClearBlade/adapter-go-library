package adapter_library

import (
	"flag"
	"fmt"
	"log"
	"os"
)

const (
	defaultLogLevel                = "info"
	defaultPlatformURL             = "http://localhost:9000"
	defaultMessagingURL            = "localhost:1883"
	defaultAdapterConfigCollection = "adapter_config"
)

var (
	args adapterArgs
)

type adapterArgs struct {
	LogLevel                string
	SystemKey               string
	SystemSecret            string
	DeviceName              string
	ActiveKey               string
	PlatformURL             string
	MessagingURL            string
	AdapterConfigCollection string
	ServiceAccount          string
	ServiceAccountToken     string
}

func ParseArguments(adapterName string) error {
	loggingInit()

	args = adapterArgs{
		LogLevel:                defaultLogLevel,
		SystemKey:               "",
		SystemSecret:            "",
		DeviceName:              adapterName,
		ActiveKey:               "",
		PlatformURL:             defaultPlatformURL,
		MessagingURL:            defaultMessagingURL,
		AdapterConfigCollection: defaultAdapterConfigCollection,
		ServiceAccount:          "",
		ServiceAccountToken:     "",
	}

	flag.StringVar(&args.SystemKey, "systemKey", "", "system key (required)")
	flag.StringVar(&args.SystemSecret, "systemSecret", "", "system secret (required)")
	flag.StringVar(&args.DeviceName, "deviceName", adapterName, "name of device (optional)")
	flag.StringVar(&args.ActiveKey, "password", "", "password (or active key) for device authentication (required)")
	flag.StringVar(&args.PlatformURL, "platformURL", defaultPlatformURL, "platform url (optional)")
	flag.StringVar(&args.MessagingURL, "messagingURL", defaultMessagingURL, "messaging URL (optional)")
	flag.StringVar(&args.LogLevel, "logLevel", defaultLogLevel, "The level of logging to use. Available levels are 'debug, 'info', 'warn', 'error', 'fatal' (optional)")
	flag.StringVar(&args.AdapterConfigCollection, "adapterConfigCollection", defaultAdapterConfigCollection, "The name of the data collection used to house adapter configuration (optional)")
	flag.Parse()

	setLoggingLevel(args.LogLevel)

	// check for any values provided by environment variables
	sysKey, ok := os.LookupEnv("CB_SYSTEM_KEY")
	if ok && args.SystemKey == "" {
		args.SystemKey = sysKey
		log.Println("[DEBUG] Using ClearBlade System Key From Environment Variable")
	}
	sysSec, ok := os.LookupEnv("CB_SYSTEM_SECRET")
	if ok && args.SystemSecret == "" {
		args.SystemSecret = sysSec
		log.Println("[DEBUG] Using ClearBlade System Secret From Environment Variable")
	}
	servAcc, ok := os.LookupEnv("CB_SERVICE_ACCOUNT")
	if ok {
		args.ServiceAccount = servAcc
		log.Println("[DEBUG] Using provided Service Account for Device Name")
	}
	token, ok := os.LookupEnv("CB_SERVICE_ACCOUNT_TOKEN")
	if ok {
		args.ServiceAccountToken = token
	}

	// verify all required fields are present
	if args.SystemKey == "" {
		return fmt.Errorf("System Key is required, can be supplied with --systemKey flag or CB_SYSTEM_KEY environment variable")
	}
	if args.SystemSecret == "" {
		return fmt.Errorf("System Secret is required, can be supplied with --systemSecret flag or CB_SYSTEM_SECRET environment variable")
	}
	if args.ActiveKey == "" && args.ServiceAccount == "" {
		return fmt.Errorf("Device Password is required when not using a Service Account, can be supplied with --password flag")
	}
	if args.ServiceAccount != "" && args.ServiceAccountToken == "" {
		return fmt.Errorf("Service Account Token is required when a Service Account is specified, this should have automatically been supplied. Check for typos then try again")
	}

	log.Printf("[DEBUG] ParseArguments - Final arguments being used: %+v\n", args)
	return nil
}

func Initialize() (*AdapterConfig, error) {
	log.Println("[INFO] Initialize - Adapter Library running needed inits")

	// auth with edge/platform
	if args.ServiceAccount != "" {
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
	adapterConfig, err := fetchAdapterConfig()
	if err != nil {
		return nil, err
	}

	return adapterConfig, nil
}
