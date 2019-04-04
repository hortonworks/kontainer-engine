package store

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"github.com/rancher/kontainer-engine/cluster"
	"github.com/rancher/kontainer-engine/utils"
	"github.com/sirupsen/logrus"
	"gopkg.in/yaml.v2"
)

const (
	caPem             = "ca.pem"
	clientKey         = "key.pem"
	clientCert        = "cert.pem"
	defaultConfigName = "config.json"
)

type CLIPersistStore struct{}

func (c CLIPersistStore) GetStatus(name string) (string, error) {
	cls, err := c.Get(name)
	if err != nil {
		return "", err
	}
	return cls.Status, nil
}

func (c CLIPersistStore) Remove(name string) error {
	fileDir, _ := getClusterPath(name)
	//Remove cluster directory
	return os.RemoveAll(fileDir)
}

func getClusterPath(name string) ( string , error ) {
	return filepath.Join(utils.ClusterStoreDir(), name), nil
}

func (c CLIPersistStore) Get(name string) (cluster.Cluster, error) {
	path, err := getClusterPath(name)
	if _, err := os.Stat(filepath.Join(path, defaultConfigName)); os.IsNotExist(err) {
		return cluster.Cluster{}, fmt.Errorf("%s not found", name)
	}
	cls := cluster.Cluster{}
	data, err := ioutil.ReadFile(filepath.Join(path, defaultConfigName))
	if err != nil {
		return cluster.Cluster{}, err
	}
	if err := json.Unmarshal(data, &cls); err != nil {
		return cluster.Cluster{}, err
	}
	return cls, nil
}

func (c CLIPersistStore) Store(cls cluster.Cluster) error {
	// store kube config file
	if err := storeConfig(cls); err != nil {
		return err
	}
	// store json config file
	fileDir, err := getClusterPath(cls.Name)
	for k, v := range map[string]string{
		cls.RootCACert:        caPem,
		cls.ClientKey:         clientKey,
		cls.ClientCertificate: clientCert,
	} {
		data, err := base64.StdEncoding.DecodeString(k)
		if err != nil {
			return fmt.Errorf("error while decoding crypto '%v': %v", k, err)
		}
		if err := utils.WriteToFile(data, filepath.Join(fileDir, v)); err != nil {
			return err
		}
	}
	data, err := json.Marshal(cls)
	if err != nil {
		return err
	}
	return utils.WriteToFile(data, filepath.Join(fileDir, defaultConfigName))
}

func (c CLIPersistStore) PersistStatus(cluster cluster.Cluster, status string) error {
	fileDir, err := getClusterPath(cluster.Name)
	if err != nil {
		return err
	}
	cluster.Status = status
	data, err := json.Marshal(cluster)
	if err != nil {
		return err
	}
	return utils.WriteToFile(data, filepath.Join(fileDir, defaultConfigName))
}

func (c CLIPersistStore) SetEnv(name string) error {
	clusters, err := GetAllClusterFromStore()
	if err != nil {
		return err
	}
	_, ok := clusters[name]
	if !ok {
		return fmt.Errorf("cluster %v can't be found", name)
	}
	clusterBaseDir, err := getClusterPath(name)
	config, err := getConfigFromFile(utils.KubeConfigFilePath(clusterBaseDir))
	if err != nil {
		return err
	}
	config.CurrentContext = name
	if err := setConfigToFile(utils.KubeConfigFilePath(clusterBaseDir), config); err != nil {
		return err
	}

	configFile := utils.KubeConfigFilePath(clusterBaseDir)
	fmt.Printf("Current context is set to %s\n", name)
	fmt.Printf("run `export KUBECONFIG=%v` or `--kubeconfig %s` to use the config file\n", configFile, configFile)
	return nil
}

func storeConfig(c cluster.Cluster) error {
	isBasicOn := false
	if c.Username != "" && c.Password != "" {
		isBasicOn = true
	}
	username, password, token := "", "", ""
	if isBasicOn {
		username = c.Username
		password = c.Password
	} else {
		token = c.ServiceAccountToken
	}

	configFilePath, err := getClusterPath(c.Name)
	configFile := utils.KubeConfigFilePath(configFilePath)
	config := KubeConfig{}
	if _, err := os.Stat(configFile); err == nil {
		data, err := ioutil.ReadFile(configFile)
		if err != nil {
			return err
		}
		if err := yaml.Unmarshal(data, &config); err != nil {
			return err
		}
	}
	config.APIVersion = "v1"
	config.Kind = "Config"
	config.CurrentContext = c.Name

	// setup clusters
	host := c.Endpoint
	if !strings.HasPrefix(host, "https://") {
		host = fmt.Sprintf("https://%s", host)
	}
	cluster := ConfigCluster{
		Cluster: DataCluster{
			CertificateAuthorityData: string(c.RootCACert),
			Server: host,
		},
		Name: c.Name,
	}
	if config.Clusters == nil || len(config.Clusters) == 0 {
		config.Clusters = []ConfigCluster{cluster}
	} else {
		exist := false
		for _, cluster := range config.Clusters {
			if cluster.Name == c.Name {
				exist = true
				break
			}
		}
		if !exist {
			config.Clusters = append(config.Clusters, cluster)
		}
	}

	// setup users
	user := ConfigUser{
		User: UserData{
			Username: username,
			Password: password,
			Token:    token,
		},
		Name: c.Name,
	}
	if config.Users == nil || len(config.Users) == 0 {
		config.Users = []ConfigUser{user}
	} else {
		exist := false
		for _, user := range config.Users {
			if user.Name == c.Name {
				exist = true
				break
			}
		}
		if !exist {
			config.Users = append(config.Users, user)
		}
	}

	// setup context
	context := ConfigContext{
		Context: ContextData{
			Cluster: c.Name,
			User:    c.Name,
		},
		Name: c.Name,
	}
	if config.Contexts == nil || len(config.Contexts) == 0 {
		config.Contexts = []ConfigContext{context}
	} else {
		exist := false
		for _, context := range config.Contexts {
			if context.Name == c.Name {
				exist = true
				break
			}
		}
		if !exist {
			config.Contexts = append(config.Contexts, context)
		}
	}

	data, err := yaml.Marshal(config)
	if err != nil {
		return err
	}

	if err := utils.WriteToFile(data, configFile); err != nil {
		return err
	}
	logrus.Debugf("KubeConfig files is saved to %s", configFile)
	logrus.Debug("Kubeconfig file\n" + string(data))

	return nil
}

func deleteConfigByName(config *KubeConfig, name string) {
	contexts := []ConfigContext{}
	for _, context := range config.Contexts {
		if context.Name != name {
			contexts = append(contexts, context)
		}
	}
	clusters := []ConfigCluster{}
	for _, cls := range config.Clusters {
		if cls.Name != name {
			clusters = append(clusters, cls)
		}
	}
	users := []ConfigUser{}
	for _, user := range config.Users {
		if user.Name != name {
			users = append(users, user)
		}
	}
	config.Contexts = contexts
	config.Clusters = clusters
	config.Users = users
}

func (c CLIPersistStore) GetKubeConfig(name string) (string, error) {
	status, err := c.GetStatus(name)
	if err != nil {
		return "", err
	}

	if status == cluster.Error {
		logrus.Errorf("Cluster %s previously failed to create", name)
		return "", fmt.Errorf("Cluster %s previously failed to create", name)
	}

	if status == cluster.PreCreating || status == cluster.Creating {
		logrus.Errorf("Cluster %s has not been created.", name)
		return "", fmt.Errorf("cluster %s has not been created", name)
	}

	fileDir, err := getClusterPath(name)
	kubeConfigPath := utils.KubeConfigFilePath(fileDir)
	if _, err := os.Stat(kubeConfigPath); os.IsNotExist(err) {
		return "", fmt.Errorf("Could not get kubeconfig cluster %s", name)
	}
	data, err := getRawKubeConfig(kubeConfigPath)
	if err != nil {
		return "", err
	}
	return string(data), err
}

func getRawKubeConfig(filepath string) ([]byte, error) {
	return ioutil.ReadFile(filepath)
}

func getConfigFromFile(filepath string) (KubeConfig, error) {
	config := KubeConfig{}
	data, err:= getRawKubeConfig(filepath)
	if err != nil {
		return KubeConfig{}, err
	}
	if err := yaml.Unmarshal(data, &config); err != nil {
		return KubeConfig{}, err
	}
	return config, nil
}

func setConfigToFile(filepath string, config KubeConfig) error {
	data, err := yaml.Marshal(config)
	if err != nil {
		return err
	}
	return utils.WriteToFile(data, filepath)
}
