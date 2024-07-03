package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/gnufied/cns-migration/pkg/cnsmigration"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
	klog "k8s.io/klog/v2"
)

func fileExists(filename string) bool {
	_, err := os.Stat(filename)
	if err == nil {
		return true
	}
	if os.IsNotExist(err) {
		return false
	}
	// Some other error occurred, possibly indicating a problem
	return false
}

func main() {
	var kubeconfig *string
	if home := homedir.HomeDir(); home != "" {
		kubeconfig = flag.String("kubeconfig", filepath.Join(home, ".kube", "config"), "(optional) absolute path to the kubeconfig file")
	} else {
		kubeconfig = flag.String("kubeconfig", "", "absolute path to the kubeconfig file")
	}

	// TODO: Do we need to accept datastore URL or just a name?
	destinationDatastore := flag.String("destination", "", "name of destination datastore")
	sourceDatastore := flag.String("source", "", "name of source datastore")

	klog.InitFlags(nil)
	flag.Parse()

	kubeConfigEnv := os.Getenv("KUBECONFIG")
	if kubeConfigEnv != "" && fileExists(kubeConfigEnv) {
		kubeconfig = &kubeConfigEnv
	}

	if destinationDatastore == nil || *destinationDatastore == "" {
		klog.Fatalf("Specify destination datastore")
	}

	if sourceDatastore == nil || *sourceDatastore == "" {
		klog.Fatalf("Specify source datastore")
	}

	fmt.Printf("KubeConfig is: %s\n", *kubeconfig)

	// use the current context in kubeconfig
	config, err := clientcmd.BuildConfigFromFlags("", *kubeconfig)
	if err != nil {
		klog.Fatalf("error building kubeconfig: %v", err)
	}

	// create the clientset
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		klog.Fatalf("error building kubernetes client: %v", err)
	}
	migrator := cnsmigration.NewCNSVolumeMigrator(clientset, *sourceDatastore, *destinationDatastore)

	err = migrator.StartMigration()
	if err != nil {
		klog.Fatalf("error migration one or more volumes: %v", err)
	}

	fmt.Println("Migrated all CNS volumes successfully")
}