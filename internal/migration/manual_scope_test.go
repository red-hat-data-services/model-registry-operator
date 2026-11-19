package migration

import (
	"context"
	"errors"
	"testing"

	"github.com/opendatahub-io/model-registry-operator/internal/controller/config"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type migrationListClient struct {
	client.Client
	namespace string
}

var stopAfterList = errors.New("stop after observing the migration list request")

func (c *migrationListClient) List(_ context.Context, _ client.ObjectList, options ...client.ListOption) error {
	c.namespace = (&client.ListOptions{}).ApplyOptions(options).Namespace
	return stopAfterList
}

func TestManualMigrationListsOnlyRegistryNamespace(t *testing.T) {
	previous := config.GetRegistriesNamespace()
	t.Cleanup(func() { _ = config.SetRegistriesNamespace(previous) })
	if err := config.SetRegistriesNamespace("registries"); err != nil {
		t.Fatal(err)
	}
	c := &migrationListClient{}
	strategy := NewManualStrategy(c, "v1alpha1", "v1beta1")
	if err := strategy.PerformMigration(context.Background(), &apiextensionsv1.CustomResourceDefinition{}); !errors.Is(err, stopAfterList) {
		t.Fatalf("unexpected migration error: %v", err)
	}
	if c.namespace != "registries" {
		t.Fatalf("migration listed namespace %q instead of registries", c.namespace)
	}
}
