package hdfs

import (
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStatFs(t *testing.T) {
	t.Skip("Stat FS does not work on Travis CI")
	client := getClient(t)

	resp, err := client.StatFs()
	require.NoError(t, err)

	hadoopPath := os.Getenv("HADOOP_HOME") + "/bin/hadoop"
	cmd := exec.Command(hadoopPath, "fs", "-df")
	stdout, err := cmd.Output()
	if err != nil {
		return
	}
	sizes := strings.Split(string(stdout), "\n")[1]
	totalsize, _ := strconv.ParseInt(strings.Fields(sizes)[1], 10, 64)
	usedsize, _ := strconv.ParseInt(strings.Fields(sizes)[2], 10, 64)

	assert.EqualValues(t, totalsize, int64(resp.Capacity()))
	assert.EqualValues(t, usedsize, int64(resp.Used()))
}
