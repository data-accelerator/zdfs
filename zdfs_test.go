package zdfs

import (
	"fmt"
	"testing"
)

func TestParseRef(t *testing.T) {
	ref := "dadi-test-registry.cn-hangzhou.cr.aliyuncs.com/tuji/wordpress:20240303_containerd_accelerated"
	fmt.Println(constructImageBlobURL(ref))
}
