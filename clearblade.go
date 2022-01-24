package adapter_library

import (
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"strings"
	"time"

	cb "github.com/clearblade/Go-SDK"
	mqttTypes "github.com/clearblade/mqtt_parsing"
	mqtt "github.com/clearblade/paho.mqtt.golang"
)

var (
	deviceClient *cb.DeviceClient
	topic        string
	mqttCallback MQTTMessageReceived
	connChannel  chan struct{}
)

type AdapterConfig struct {
	AdapterSettings string
	TopicRoot       string
}

type ConnectionStatus struct {
	Status       string `json:"status"`
	ErrorMessage string `json:"error_message,omitempty"`
	Timestamp    string `json:"timestamp"`
}

type MQTTMessageReceived func(*mqttTypes.Publish)

func ConnectMQTT(t string, mqttCB MQTTMessageReceived) error {
	log.Println("[INFO] ConnectMQTT - initializing and connecting to ClearBlade MQTT broker")
	topic = t
	mqttCallback = mqttCB
	callbacks := cb.Callbacks{OnConnectionLostCallback: onConnectLost, OnConnectCallback: onConnect}
	connChannel = make(chan struct{})
	if err := deviceClient.InitializeMQTTWithCallback(Args.DeviceName+"-"+strconv.Itoa(rand.Intn(10000)), "", 30, nil, nil, &callbacks); err != nil {
		return fmt.Errorf("Failed to initialize MQTT connection: %s", err.Error())
	}
	<-connChannel
	return nil
}

func Publish(topic string, message []byte) error {
	return deviceClient.Publish(topic, message, 0)
}

func PublishGetToken(topic string, message []byte) (mqtt.Token, error) {
	return deviceClient.PublishGetToken(topic, message, 0)
}

func authWithDevice() error {
	log.Println("[INFO] authWithDevice - Authenticating with ClearBlade Edge or Platform as a Device")
	log.Println("[ERROR] authWithDevice - This functionality is depreciated! Please use a Device Service Account instead")
	deviceClient = cb.NewDeviceClientWithAddrs(Args.PlatformURL, Args.MessagingURL, Args.SystemKey, Args.SystemSecret, Args.DeviceName, Args.ActiveKey)
	_, err := deviceClient.Authenticate()
	return err
}

func authWithServiceAccount() error {
	log.Println("[INFO] authWithServiceAccount - Authenticating with ClearBlade Edge or Platform using a Service Account")
	deviceClient = cb.NewDeviceClientWithServiceAccountAndAddrs(Args.PlatformURL, Args.MessagingURL, Args.SystemKey, Args.SystemSecret, Args.ServiceAccount, Args.ServiceAccountToken)
	return nil
}

func fetchAdapterConfig() (*AdapterConfig, error) {
	log.Println("[INFO] fetchAdapterConfig - Retrieving adapter config")
	config := &AdapterConfig{
		TopicRoot: Args.DeviceName,
	}

	//Retrieve the adapter configuration row
	query := cb.NewQuery()
	if Args.ServiceAccount != "" {
		log.Printf("[DEBUG] fetchAdapterConfig - Fetching config row with adapter_name: %s\n", Args.ServiceAccount)
		query.EqualTo("adapter_name", Args.ServiceAccount)
	} else {
		log.Printf("[DEBUG] fetchAdapterConfig - Fetching config row with adapter_name: %s\n", Args.DeviceName)
		query.EqualTo("adapter_name", Args.DeviceName)
	}

	log.Println("[DEBUG] fetchAdapterConfig - Executing query against table " + Args.AdapterConfigCollection)
	results, err := deviceClient.GetDataByName(Args.AdapterConfigCollection, query)
	if err != nil {
		log.Printf("[ERROR] fetchAdapterConfig - Error retrieving adapter configuration: %s\n", err.Error())
		log.Println("[ERROR] fetchAdapterConfig - Retrying in 30 seconds...")
		time.Sleep(time.Second * 30)
		return fetchAdapterConfig()
	}
	data := results["DATA"].([]interface{})
	if len(data) > 0 {
		log.Println("[INFO] fetchAdapterConfig - Adapter config retrieved")
		configData := data[0].(map[string]interface{})

		//MQTT topic root
		if configData["topic_root"] != nil && configData["topic_root"] != "" {
			config.TopicRoot = configData["topic_root"].(string)
		}
		log.Printf("[DEBUG] fetchAdapterConfig - TopicRoot set to %s\n", config.TopicRoot)

		//adapter_settings
		if configData["adapter_settings"] != nil {
			config.AdapterSettings = configData["adapter_settings"].(string)
		} else {
			log.Println("[INFO] fetchAdapterConfig - Settings are nil.")
		}
	} else {
		log.Println("[INFO] fetchAdapterConfig - No rows returned. Using defaults")
	}

	log.Printf("[DEBUG] fetchAdapterConfig - Successfully received and parsed adapter config: %+v\n", config)
	return config, nil
}

func PublishStatus(topic string, json []byte) (mqtt.Token, error) {
	log.Printf("[INFO] PublishStatus - Setting status on topic: %s\n", topic)

	log.Printf("[DEBUG] PublishStatus - Publishing to topic %s\n", topic)
	token, err := PublishGetToken(topic, json)
	if err != nil {
		log.Printf("[ERROR] Failed to publish MQTT message to topic %s: %s\n", topic, err.Error())
	}

	log.Printf("[DEBUG] PublishStatus - Successfully sent status message to topic: %s\n", topic)
	return token, err
}

func onConnectLost(client mqtt.Client, connerr error) {
	if Args.FatalOnDisconnect == "true" {
		log.Fatalf("[ERROR] onConnectLost - Connection to MQTT broker was lost: %s\n", connerr.Error())
		if Args.ServiceAccount == "" {
			log.Fatalln("[FATAL] onConnectLost - MQTT Connection was lost and no Device Service Account is being used. Stopping Adapter to force device reauth (this can be avoided by using Device Service Accounts)")
		}
	} else {
		log.Printf("[ERROR] onConnectLost - Connection to MQTT broker was lost: %s\n", connerr.Error())
		if Args.ServiceAccount == "" {
			log.Println("[FATAL] onConnectLost - MQTT Connection was lost and no Device Service Account is being used. Stopping Adapter to force device reauth (this can be avoided by using Device Service Accounts)")
		}
	}
}

func onConnect(client mqtt.Client) {
	log.Println("[INFO] onConnect - Connected to ClearBlade Platform MQTT broker")
	connChannel <- struct{}{}
	if topic != "" && mqttCallback != nil {
		// this is a bit fragile, relying on a specific error message text to check if error was lack of permissions or not, it it's not we want to retry,
		// but if it is we want to quit out because this won't ever work
		log.Println("[INFO] onConnect - Subscribing to provided topic " + topic)
		var cbSubChannel <-chan *mqttTypes.Publish
		var err error
		for cbSubChannel, err = deviceClient.Subscribe(topic, 0); err != nil; {
			if strings.Contains(err.Error(), "Connection lost before Subscribe completed") {
				log.Fatalf("[FATAL] onConnect - Ensure your device has subscribe permissionns to topic %s\n", topic)
			} else {
				log.Printf("[ERROR] onConnect - Error subscribing to MQTT topic: %s\n", err.Error())
				log.Println("[ERROR] onConnect - Retrying in 30 seconds...")
				time.Sleep(time.Duration(30 * time.Second))
				cbSubChannel, err = deviceClient.Subscribe(topic, 0)
			}
		}
		go cbMessageListener(cbSubChannel)
	} else {
		log.Println("[INFO] onConnect - no topic or mqtt callback supplied, will not subscribe to any MQTT topics")
	}
}

func cbMessageListener(onPubChannel <-chan *mqttTypes.Publish) {
	for {
		select {
		case message, ok := <-onPubChannel:
			if ok {
				log.Printf("[DEBUG] cbMessageListener - message received on topic %s with payload %s\n", message.Topic.Whole, string(message.Payload))
				mqttCallback(message)
			}
		}
	}
}
