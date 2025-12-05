package zdfs

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/containerd/accelerated-container-image/pkg/types"
	"github.com/containerd/containerd/v2/core/snapshots"
)

func TestParseRef(t *testing.T) {
	const (
		testRef  = "dadi-test-registry.cn-hangzhou.cr.example.test/tuji/wordpress:20240303_containerd_accelerated"
		expected = "https://dadi-test-registry.cn-hangzhou.cr.example.test/v2/tuji/wordpress/blobs"
	)

	actual, err := constructImageBlobURL(testRef)
	if err != nil {
		t.Fatal(err)
	}
	if actual != expected {
		t.Fatalf("expected: %s, actual: %s", expected, actual)
	}
}

func TestPrepareOverlayBDSpec(t *testing.T) {
	ctx := context.Background()

	testdir := t.TempDir()
	snPath := func(id string) string {
		return filepath.Join(testdir, id)
	}
	const (
		dgst      = "sha256:0000000000000000000000000000000000000000000000000000000000000000"
		domain    = "registry-1.example.test"
		domainNew = "registry-2.example.test"
	)

	expectedConfig := func(repoBlobUrl string) types.OverlayBDBSConfig {
		return types.OverlayBDBSConfig{
			RepoBlobURL: repoBlobUrl,
			Lowers: []types.OverlayBDBSConfigLower{
				{
					File: overlaybdBaseLayer,
				},
				{
					Digest: dgst,
					Dir:    filepath.Join(snPath("0"), "block"),
				},
			},
			ResultFile: filepath.Join(overlaybdInitDebuglogPath(snPath("0"))),
		}
	}

	testcases := []struct {
		name     string
		imageRef string
		expected types.OverlayBDBSConfig
	}{
		{
			name:     "without image ref",
			imageRef: "",
			expected: expectedConfig(fmt.Sprintf("https://%s/v2/test/blobs", domain)),
		},
		{
			name:     "with image ref",
			imageRef: domainNew + "/test:latest",
			expected: expectedConfig(fmt.Sprintf("https://%s/v2/test/blobs", domainNew)),
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			if err := os.RemoveAll(testdir); err != nil {
				t.Fatal(err)
			}
			if err := os.MkdirAll(filepath.Join(snPath("0"), "fs"), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.MkdirAll(filepath.Join(snPath("0"), "block"), 0o755); err != nil {
				t.Fatal(err)
			}

			// prepare snapshot dir (image_ref, .oss_url ...)
			files := []string{iNewFormat, zdfsChecksumFile, zdfsOssurlFile, zdfsOssDataSizeFile, zdfsOssTypeFile}
			for _, file := range files {
				if err := os.WriteFile(filepath.Join(snPath("0"), "fs", file), nil, 0o644); err != nil {
					t.Fatal(err)
				}
			}
			ossURL := fmt.Sprintf("https://%s/v2/test/blobs/%s", domain, dgst)
			if err := os.WriteFile(filepath.Join(snPath("0"), "fs", zdfsOssurlFile), []byte(ossURL), 0o644); err != nil {
				t.Fatal(err)
			}
			if tc.imageRef != "" {
				t.Logf("using image ref: %s", tc.imageRef)
				if err := os.WriteFile(filepath.Join(snPath("0"), "image_ref"), []byte(tc.imageRef), 0o644); err != nil {
					t.Fatal(err)
				}
			}

			ok, err := PrepareOverlayBDSpec(ctx, "test-key", "0", snPath("0"), snapshots.Info{}, snPath)
			if err != nil {
				t.Fatal(err)
			}
			if !ok {
				t.Errorf("expected: true, got: %v", ok)
			}

			var config types.OverlayBDBSConfig
			b, err := os.ReadFile(overlaybdConfPath(snPath("0")))
			if err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(b, &config); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(tc.expected, config) {
				t.Errorf("\nexpected: %+v\nactual  : %+v\n", tc.expected, config)
			}

			out, _ := json.MarshalIndent(config, "", "  ")
			t.Logf("construct config: \n%s", out)
		})
	}
}
