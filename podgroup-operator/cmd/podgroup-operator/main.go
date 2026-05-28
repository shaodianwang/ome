package main

import (
	"context"
	"log"

	controller "github.com/alsiens/podgroup-operator/pkg/controller/v1alpha1"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

func main() {
	cfg, err := rest.InClusterConfig()
	if err != nil {
		log.Fatalf("load in-cluster kube config: %v", err)
	}

	kube, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		log.Fatalf("create kubernetes client: %v", err)
	}
	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		log.Fatalf("create dynamic client: %v", err)
	}

	controller.New(kube, dyn).Run(context.Background())
}
