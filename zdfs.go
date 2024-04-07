package zdfs

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/containerd/accelerated-container-image/pkg/types"
	"github.com/containerd/containerd/snapshots"
	"github.com/containerd/containerd/snapshots/storage"
	"github.com/containerd/continuity"
	"github.com/distribution/reference"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

var (
	zdfsIsReady  bool       //indicate if zdfs' binaries or rpms are ready
	pouchDirLock sync.Mutex //Used by PrepareMetaForPouch(..) to guarantee thread safe during operating dirs or files
	blockEngine  string
)

const (
	zdfsMetaDir         = "zdfsmeta"               //meta dir that contains the dadi image meta files
	iNewFormat          = ".aaaaaaaaaaaaaaaa.lsmt" //characteristic file of dadi image
	zdfsChecksumFile    = ".checksum_file"         //file containing the checksum data if each dadi layer file to guarantee data consistent
	zdfsOssurlFile      = ".oss_url"               //file containing the address of layer file
	zdfsOssDataSizeFile = ".data_size"             //file containing the size of layer file
	zdfsOssTypeFile     = ".type"                  //file containing the type, such as layern, commit(layer file on local dir), oss(layer file is in oss
	zdfsTrace           = ".trace"

	overlaybdBaseLayer = "/opt/overlaybd/baselayers/.commit"
)

// If error is nil, the existence is valid.
// If error is not nil, the existence is invalid. Can't make sure if path exists.
func pathExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil //path exists.
	}
	if os.IsNotExist(err) {
		return false, nil //pash doen't exist.
	}
	return false, err //can't make sure if path exists.
}

func hasOverlaybdBlobRef(dir string) (bool, error) {
	fileNames := []string{iNewFormat, zdfsChecksumFile, zdfsOssurlFile, zdfsOssDataSizeFile, zdfsOssTypeFile}
	for _, name := range fileNames {
		fullPath := path.Join(dir, name)
		b, err := pathExists(fullPath)
		if err != nil {
			return false, fmt.Errorf("LSMD ERROR failed to check if %s exists. err:%s", fullPath, err)
		}

		if !b {
			return false, nil
		}
	}
	return true, nil
}

func overlaybdConfPath(dir string) string {
	return filepath.Join(dir, "block", "config.v1.json")
}

func overlaybdInitDebuglogPath(dir string) string {
	return filepath.Join(dir, zdfsMetaDir, "init-debug.log")
}

func isOverlaybdLayer(dir string) (bool, error) {
	exists, _ := pathExists(overlaybdConfPath(dir))
	if exists {
		return true, nil
	}

	b, err := hasOverlaybdBlobRef(path.Join(dir, "fs"))
	if err != nil {
		logrus.Errorf("LSMD ERROR failed to IsZdfsLayerInApplyDiff(dir%s), err:%s", dir, err)
		return false, fmt.Errorf("LSMD ERROR failed to IsZdfsLayerInApplyDiff(dir%s), err:%s", dir, err)
	}
	return b, nil
}

func getTrimStringFromFile(filePath string) (string, error) {
	data, err := ioutil.ReadFile(filePath)
	if err != nil {
		return "", err
	}
	return strings.Trim(string(data), " \n"), nil
}

func updateSpec(dir, recordTracePath string) error {
	bsConfig, err := loadBackingStoreConfig(dir)
	if err != nil {
		return err
	}
	if recordTracePath == bsConfig.RecordTracePath {
		// No need to update
		return nil
	}
	bsConfig.RecordTracePath = recordTracePath
	return atomicWriteOverlaybdTargetConfig(dir, bsConfig)
}

func GetBlobRepoDigest(dir string) (string, string, error) {
	// get repoUrl from .oss_url
	url, err := getTrimStringFromFile(path.Join(dir, zdfsOssurlFile))
	if err != nil {
		return "", "", err
	}

	idx := strings.LastIndex(url, "/")
	if !strings.HasPrefix(url[idx+1:], "sha256") {
		return "", "", fmt.Errorf("can't parse sha256 from url %s", url)
	}

	return url[0:idx], url[idx+1:], nil
}

func GetBlobSize(dir string) (uint64, error) {
	str, err := getTrimStringFromFile(path.Join(dir, zdfsOssDataSizeFile))
	if err != nil {
		return 0, err
	}
	return strconv.ParseUint(str, 10, 64)
}

func constructImageBlobURL(ref string) (string, error) {
	refspec, err := reference.ParseNamed(ref)
	if err != nil {
		return "", errors.Wrapf(err, "invalid repo url %s", ref)
	}

	host := reference.Domain(refspec)
	// repo := strings.TrimPrefix(refspec.Locator, host+"/")
	repo := reference.Path(reference.TrimNamed(refspec))
	return "https://" + path.Join(host, "v2", repo) + "/blobs", nil
}

// loadBackingStoreConfig loads overlaybd target config.
func loadBackingStoreConfig(dir string) (*types.OverlayBDBSConfig, error) {
	confPath := overlaybdConfPath(dir)
	data, err := ioutil.ReadFile(confPath)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to read config(path=%s) of snapshot %s", confPath, dir)
	}

	var configJSON types.OverlayBDBSConfig
	if err := json.Unmarshal(data, &configJSON); err != nil {
		return nil, errors.Wrapf(err, "failed to unmarshal data(%s)", string(data))
	}

	return &configJSON, nil
}

func atomicWriteOverlaybdTargetConfig(dir string, configJSON *types.OverlayBDBSConfig) error {
	data, err := json.Marshal(configJSON)
	if err != nil {
		return errors.Wrapf(err, "failed to marshal %+v configJSON into JSON", configJSON)
	}

	confPath := overlaybdConfPath(dir)
	if err := continuity.AtomicWriteFile(confPath, data, 0600); err != nil {
		return errors.Wrapf(err, "failed to commit the overlaybd config on %s", confPath)
	}
	return nil
}

func constructSpec(dir, parent, repo, digest string, size uint64, recordTracePath string) error {
	configJSON := types.OverlayBDBSConfig{
		Lowers:     []types.OverlayBDBSConfigLower{},
		ResultFile: overlaybdInitDebuglogPath(dir),
	}

	configJSON.RepoBlobURL = repo
	if parent == "" {
		configJSON.Lowers = append(configJSON.Lowers, types.OverlayBDBSConfigLower{
			File: overlaybdBaseLayer,
		})
	} else {
		parentConfJSON, err := loadBackingStoreConfig(parent)
		if err != nil {
			return err
		}
		if repo == "" {
			configJSON.RepoBlobURL = parentConfJSON.RepoBlobURL
		}
		configJSON.Lowers = parentConfJSON.Lowers
	}

	configJSON.RecordTracePath = recordTracePath
	configJSON.Lowers = append(configJSON.Lowers, types.OverlayBDBSConfigLower{
		Digest: digest,
		Size:   int64(size),
		Dir:    path.Join(dir, "block"),
	})
	return atomicWriteOverlaybdTargetConfig(dir, &configJSON)
}

func PrepareOverlayBDSpec(ctx context.Context, key, id, dir string, info snapshots.Info, snPath func(string) string) (bool, error) {

	if b, err := isOverlaybdLayer(dir); !b {
		return false, nil
	} else if err != nil {
		return false, err
	}
	s, _ := storage.GetSnapshot(ctx, key)
	lowers := func() []string {
		ret := []string{}
		for _, id := range s.ParentIDs {
			ret = append(ret, snPath(id))
		}
		return ret
	}()
	makeConfig := func(dir string, parent string) error {
		logrus.Infof("ENTER makeConfig(dir: %s, parent: %s)", dir, parent)
		dstDir := path.Join(dir, "block")

		repo, digest, err := GetBlobRepoDigest(dstDir)
		if err != nil {
			return err
		}

		refPath := path.Join(dir, path.Join(dir, "image_ref"))
		if b, _ := pathExists(refPath); b {
			img, _ := os.ReadFile(refPath)
			imageRef := string(img)
			logrus.Infof("read imageRef from label.CRIImageRef: %s", imageRef)
			repo, _ = constructImageBlobURL(imageRef)
		}
		logrus.Infof("construct repoBlobUrl: %s", repo)

		size, _ := GetBlobSize(dstDir)
		if err := constructSpec(dir, parent, repo, digest, size, ""); err != nil {
			return err
		}
		return nil
	}

	doDir := func(dir string, parent string) error {
		dstDir := path.Join(dir, zdfsMetaDir)
		//1.check if the dir exists. Create the dir only when dir doesn't exist.
		b, err := pathExists(dstDir)
		if err != nil {
			logrus.Errorf("LSMD ERROR PathExists(%s) err:%s", dstDir, err)
			return err
		}

		if b {
			configPath := overlaybdConfPath(dir)
			configExists, err := pathExists(configPath)
			if err != nil {
				logrus.Errorf("LSMD ERROR PathExists(%s) err:%s", configPath, err)
				return err
			}
			if configExists {
				logrus.Infof("%s has been created yet.", configPath)
				return updateSpec(dir, "")
			}
			// config.v1.json does not exist, for early pulled layers
			return makeConfig(dir, parent)
		}

		b, _ = pathExists(path.Join(dir, "block", "config.v1.json"))
		if b {
			// is new dadi format
			return nil
		}

		//2.create tmpDir in dir
		tmpDir, err := os.MkdirTemp(dir, "temp_for_prepare_dadimeta")
		if err != nil {
			logrus.Errorf("LSMD ERROR os.MkdirTemp(%s.) err:%s", tmpDir, err)
			return err
		}

		//3.copy meta files to tmpDir)
		srcDir := path.Join(dir, "fs")
		if err := copyPulledZdfsMetaFiles(srcDir, tmpDir); err != nil {
			logrus.Errorf("failed to copyPulledZdfsMetaFiles(%s, %s), err:%s", srcDir, tmpDir, err)
			return err
		}

		blockDir := path.Join(dir, "block")
		if err := copyPulledZdfsMetaFiles(srcDir, blockDir); err != nil {
			logrus.Errorf("failed to copyPulledZdfsMetaFiles(%s, %s), err:%s", srcDir, blockDir, err)
			return err
		}

		//4.rename tmpDir to zdfsmeta
		if err = os.Rename(tmpDir, dstDir); err != nil {
			return err
		}

		//5.generate config.v1.json
		return makeConfig(dir, parent)
	}

	num := len(lowers)
	parent := ""
	for m := 0; m < num; m++ {
		dir := lowers[num-m-1]
		if err := doDir(dir, parent); err != nil {
			logrus.Errorf("LSMD ERROR doDir(%s) err:%s", dir, err)
			return true, err
		}
		parent = dir
	}

	return true, doDir(snPath(id), parent)
}

func copyPulledZdfsMetaFiles(srcDir, dstDir string) error {
	fileNames := []string{iNewFormat, zdfsChecksumFile, zdfsOssurlFile, zdfsOssDataSizeFile, zdfsOssTypeFile, zdfsTrace}
	for _, name := range fileNames {
		srcPath := path.Join(srcDir, name)
		if _, err := os.Stat(srcPath); err != nil && os.IsNotExist(err) {
			continue
		}
		data, err := os.ReadFile(srcPath)
		if err != nil {
			logrus.Errorf("LSMD ERROR ioutil.ReadFile(srcDir:%s, name:%s) dstDir:%s, err:%s", srcDir, name, dstDir, err)
			return err
		}
		if err := os.WriteFile(path.Join(dstDir, name), data, 0666); err != nil {
			logrus.Errorf("LSMD ERROR ioutil.WriteFile(path.Join(dstDir:%s, name:%s) srcDir:%s err:%s", dstDir, name, srcDir, err)
			return err
		}
	}
	return nil
}
