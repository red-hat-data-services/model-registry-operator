package main

import (
	"bytes"
	"context"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	registryv1beta1 "github.com/opendatahub-io/model-registry-operator/api/v1beta1"
	"github.com/opendatahub-io/model-registry-operator/internal/controller"
	"github.com/opendatahub-io/model-registry-operator/internal/controller/config"
	appsv1 "k8s.io/api/apps/v1"
	authv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

func renderManifests(t *testing.T, overlay string) []*unstructured.Unstructured {
	t.Helper()
	directory := "../config/" + overlay
	if filepath.IsAbs(overlay) {
		directory = overlay
	}
	out, err := exec.Command("../bin/kustomize", "build", directory).CombinedOutput()
	if err != nil {
		t.Fatalf("render manifests (run make kustomize first): %v\n%s", err, out)
	}
	decoder := yaml.NewYAMLOrJSONDecoder(bytes.NewReader(out), 4096)
	var objects []*unstructured.Unstructured
	for {
		obj := &unstructured.Unstructured{}
		if err := decoder.Decode(obj); err == io.EOF {
			return objects
		} else if err != nil {
			t.Fatal(err)
		}
		objects = append(objects, obj)
	}
}

func manifest(t *testing.T, objects []*unstructured.Unstructured, kind, name string) *unstructured.Unstructured {
	t.Helper()
	for _, obj := range objects {
		if obj.GetKind() == kind && obj.GetName() == name {
			return obj
		}
	}
	t.Fatalf("missing %s %s", kind, name)
	return nil
}

func TestDeploymentIsolation(t *testing.T) {
	customConfig := t.TempDir()
	if err := os.CopyFS(customConfig, os.DirFS("../config")); err != nil {
		t.Fatal(err)
	}
	paramsPath := filepath.Join(customConfig, "overlays/odh/params.env")
	params, err := os.ReadFile(paramsPath)
	if err != nil {
		t.Fatal(err)
	}
	params = bytes.ReplaceAll(params, []byte("REGISTRIES_NAMESPACE=rhoai-model-registries"), []byte("REGISTRIES_NAMESPACE=custom-registries"))
	if err := os.WriteFile(paramsPath, params, 0600); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ overlay, namespace string }{
		{"default", "model-registries"},
		{"overlays/odh", "rhoai-model-registries"},
		{filepath.Join(customConfig, "overlays/odh"), "custom-registries"},
	} {
		t.Run(tc.overlay, func(t *testing.T) {
			objects := renderManifests(t, tc.overlay)
			deployment := &appsv1.Deployment{}
			if err := scheme.Convert(manifest(t, objects, "Deployment", "model-registry-operator-controller-manager"), deployment, nil); err != nil {
				t.Fatal(err)
			}
			foundNamespace := false
			for _, container := range deployment.Spec.Template.Spec.Containers {
				for _, env := range container.Env {
					if env.Name == config.RegistriesNamespace && env.Value == tc.namespace {
						foundNamespace = true
					}
				}
			}
			if !foundNamespace {
				t.Fatal("manager namespace does not match the workload Role")
			}
			if deployment.Namespace != "model-registry-operator-system" || deployment.Namespace == tc.namespace {
				t.Fatalf("controller namespace is not isolated: %s", deployment.Namespace)
			}
			binding := &rbacv1.RoleBinding{}
			if err := scheme.Convert(manifest(t, objects, "RoleBinding", "model-registry-operator-registry-rolebinding"), binding, nil); err != nil {
				t.Fatal(err)
			}
			if binding.Namespace != tc.namespace || binding.RoleRef.Kind != "Role" || len(binding.Subjects) != 1 ||
				binding.Subjects[0].Namespace != deployment.Namespace || binding.Subjects[0].Name != deployment.Spec.Template.Spec.ServiceAccountName {
				t.Fatalf("incorrect registry binding: %+v", binding)
			}
			if role := manifest(t, objects, "Role", binding.RoleRef.Name); role.GetNamespace() != tc.namespace {
				t.Fatalf("workload Role namespace = %s", role.GetNamespace())
			}
			if tc.overlay != "default" && manifest(t, objects, "Template", "jobs-async-upload-s3-to-oci-template").GetNamespace() != "redhat-ods-applications" {
				t.Fatal("import-job template must remain in the applications namespace")
			}
		})
	}
}

// Exercise authorization with the rendered permissions, rather than an admin
// client, and start the real controllers to catch accidentally cluster-wide watches.
func TestRestrictedManagerRBAC(t *testing.T) {
	objects := renderManifests(t, "overlays/odh")
	environment := &envtest.Environment{
		Scheme:                scheme,
		CRDDirectoryPaths:     []string{"../config/crd/bases"},
		ErrorIfCRDPathMissing: true,
		BinaryAssetsDirectory: filepath.Join("..", "bin", "k8s", "1.28.0-"+runtime.GOOS+"-"+runtime.GOARCH),
	}
	adminConfig, err := environment.Start()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := environment.Stop(); err != nil {
			t.Error(err)
		}
	})
	ctx := context.Background()
	admin, err := client.New(adminConfig, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatal(err)
	}
	const operatorNamespace = "model-registry-operator-system"
	const registryNamespace = "rhoai-model-registries"
	for _, namespace := range []string{operatorNamespace, registryNamespace, "redhat-ods-applications", "unrelated"} {
		if err := admin.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}}); err != nil {
			t.Fatal(err)
		}
	}
	for _, obj := range objects {
		switch obj.GetKind() {
		case "ServiceAccount", "Role", "ClusterRole", "RoleBinding", "ClusterRoleBinding":
			if err := admin.Create(ctx, obj); err != nil {
				t.Fatal(err)
			}
		}
	}
	operatorConfig := rest.CopyConfig(adminConfig)
	operatorConfig.Impersonate = rest.ImpersonationConfig{
		UserName: "system:serviceaccount:" + operatorNamespace + ":model-registry-operator-controller-manager",
		Groups:   []string{"system:authenticated", "system:serviceaccounts", "system:serviceaccounts:" + operatorNamespace},
	}
	operator, err := client.New(operatorConfig, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatal(err)
	}
	for _, namespace := range []string{registryNamespace, "unrelated", operatorNamespace, ""} {
		for _, tc := range []struct {
			group, resource, verb string
			allowed               bool
		}{
			{"", "secrets", "get", namespace == registryNamespace},
			{"", "secrets", "create", namespace == registryNamespace},
			{"", "secrets", "list", namespace == registryNamespace},
			{"", "secrets", "watch", namespace == registryNamespace},
			{"apps", "deployments", "create", namespace == registryNamespace},
			{"apps", "deployments", "update", namespace == registryNamespace},
			{"", "serviceaccounts/token", "create", false},
			{"rbac.authorization.k8s.io", "roles", "escalate", false},
		} {
			t.Run(namespace+"/"+tc.resource+"/"+tc.verb, func(t *testing.T) {
				assertAccess(t, ctx, operator, namespace, tc.group, tc.resource, tc.verb, tc.allowed)
			})
		}
	}
	// A namespace editor can still create application Pods, but cannot create
	// Pods or read credentials in the namespace holding the controller identity.
	if err := admin.Create(ctx, &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: "application-editor", Namespace: "redhat-ods-applications"},
		Subjects:   []rbacv1.Subject{{Kind: "User", APIGroup: rbacv1.GroupName, Name: "application-editor"}},
		// envtest has no aggregation controller; bind the built-in edit rules directly.
		RoleRef: rbacv1.RoleRef{Kind: "ClusterRole", APIGroup: rbacv1.GroupName, Name: "system:aggregate-to-edit"},
	}); err != nil {
		t.Fatal(err)
	}
	editorConfig := rest.CopyConfig(adminConfig)
	editorConfig.Impersonate = rest.ImpersonationConfig{UserName: "application-editor", Groups: []string{"system:authenticated"}}
	editor, err := client.New(editorConfig, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatal(err)
	}
	assertAccess(t, ctx, editor, "redhat-ods-applications", "", "pods", "create", true)
	assertAccess(t, ctx, editor, operatorNamespace, "", "pods", "create", false)
	assertAccess(t, ctx, editor, operatorNamespace, "", "secrets", "get", false)

	if err := config.SetRegistriesNamespace(registryNamespace); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = config.SetRegistriesNamespace("") })
	config.SetDefaultDomain("example.test", nil, false)
	templates, err := config.ParseTemplates()
	if err != nil {
		t.Fatal(err)
	}
	mgr, err := ctrl.NewManager(operatorConfig, ctrl.Options{
		Scheme:  scheme,
		Cache:   cache.Options{DefaultNamespaces: map[string]cache.Config{registryNamespace: {}}},
		Client:  client.Options{Cache: &client.CacheOptions{DisableFor: []client.Object{&corev1.Secret{}}}},
		Metrics: metricsserver.Options{BindAddress: "0"}, HealthProbeBindAddress: "0",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := (&controller.ModelRegistryReconciler{
		Client: mgr.GetClient(), Scheme: scheme, Template: templates,
		Recorder: mgr.GetEventRecorderFor("test-registry"),
	}).SetupWithManager(mgr); err != nil {
		t.Fatal(err)
	}
	if err := (&controller.ModelCatalogReconciler{
		Client: mgr.GetClient(), Scheme: scheme, Template: templates,
		TargetNamespace: registryNamespace, Enabled: true,
		Recorder: mgr.GetEventRecorderFor("test-catalog"),
	}).SetupWithManager(mgr); err != nil {
		t.Fatal(err)
	}
	managerCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- mgr.Start(managerCtx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(10 * time.Second):
			t.Error("manager did not stop")
		}
	})
	if err := wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, 20*time.Second, true, func(ctx context.Context) (bool, error) {
		err := admin.Get(ctx, client.ObjectKey{Namespace: registryNamespace, Name: "model-catalog-oauth-cookie-secret"}, &corev1.Secret{})
		return err == nil, client.IgnoreNotFound(err)
	}); err != nil {
		t.Fatalf("catalog did not reconcile with restricted RBAC: %v", err)
	}
	registry := &registryv1beta1.ModelRegistry{
		ObjectMeta: metav1.ObjectMeta{Name: "private-registry", Namespace: registryNamespace},
		Spec: registryv1beta1.ModelRegistrySpec{
			MySQL: &registryv1beta1.MySQLConfig{
				Host: "database", Database: "registry", Username: "registry",
				PasswordSecret: &registryv1beta1.SecretKeyValue{Name: "database-credentials", Key: "password"},
			},
		},
	}
	if err := admin.Create(ctx, registry); err != nil {
		t.Fatal(err)
	}
	deployment := &appsv1.Deployment{}
	if err := wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, 20*time.Second, true, func(ctx context.Context) (bool, error) {
		err := admin.Get(ctx, client.ObjectKeyFromObject(registry), deployment)
		return err == nil, client.IgnoreNotFound(err)
	}); err != nil {
		t.Fatalf("registry did not reconcile with restricted RBAC: %v", err)
	}

}

func assertAccess(t *testing.T, ctx context.Context, c client.Client, namespace, group, resource, verb string, allowed bool) {
	t.Helper()
	parts := strings.SplitN(resource, "/", 2)
	attributes := &authv1.ResourceAttributes{Namespace: namespace, Group: group, Resource: parts[0], Verb: verb}
	if len(parts) == 2 {
		attributes.Subresource = parts[1]
	}
	review := &authv1.SelfSubjectAccessReview{Spec: authv1.SelfSubjectAccessReviewSpec{
		ResourceAttributes: attributes,
	}}
	if err := c.Create(ctx, review); err != nil {
		t.Fatal(err)
	}
	if review.Status.Allowed != allowed {
		t.Fatalf("%s %s/%s in %q: allowed=%v, want %v", verb, group, resource, namespace, review.Status.Allowed, allowed)
	}
}
