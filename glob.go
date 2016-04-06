package hdfs

import (
	"fmt"
	"os"
	"path"
	"regexp"
	"strings"
)

var magicRe = regexp.MustCompile("[*?[{}]")
var expanderRe = regexp.MustCompile("{(.*?)}")

func ExpandPaths(paths []string, returnPaths []string) []string {
	for _, p := range paths {
		hasExpander := expanderRe.MatchString(p)
		if hasExpander {
			var globList []string
			firstOpen := strings.Index(p, "{") + 1
			firstClose := strings.Index(p, "}")
			opts := strings.Split(p[firstOpen:firstClose], ",")
			templateArray := []string{p[:firstOpen-1], "%s", p[firstClose+1:]}
			template := strings.Join(templateArray, "")
			for _, opt := range opts {
				globList = append(globList, fmt.Sprintf(template, opt))
			}
			returnPaths = ExpandPaths(globList, returnPaths)
		} else {
			returnPaths = append(returnPaths, p)
		}
	}
	return returnPaths
}

func ExpandPath(p string) []string {
	return ExpandPaths([]string{p}, []string{})
}

func GlobHasMagic(element string) bool {
	matched := magicRe.MatchString(element)
	return matched
}

func (c *Client) GlobFind(globPath string) ([]os.FileInfo, error) {
	return c.GetGlob(globPath, []os.FileInfo{})
}

func (c *Client) GetGlob(originalGlobPath string, pathsArray []os.FileInfo) ([]os.FileInfo, error) {
	var firstMagic int
	var checkPath string
	var rest string

	for _, globPath := range ExpandPath(originalGlobPath) {

		globElements := strings.Split(globPath, "/")
		for i, element := range globElements {
			if GlobHasMagic(element) {
				firstMagic = i
				break
			}
		}

		if firstMagic == 1 {
			checkPath = "/"
		} else if firstMagic == 0 {
			checkPath = globPath
		} else {
			checkPath = strings.Join(globElements[:firstMagic], "/")
		}

		magicString := globElements[firstMagic]

		restElements := globElements[firstMagic+1:]
		if len(restElements) == 1 {
			rest = restElements[0]
		} else {
			rest = strings.Join(restElements, "/")
		}

		fileInfo, err := c.Stat(checkPath)
		if err == nil {
			if fileInfo.IsDir() {
				dirInfo, _ := c.ReadDir(checkPath)
				for _, node := range dirInfo {
					var nextPathArray []string
					var nextPathStat os.FileInfo

					if len(rest) > 0 {
						nextPathArray = []string{checkPath, node.Name(), rest}
					} else {
						nextPathArray = []string{checkPath, node.Name()}
					}
					nextPath := strings.Join(nextPathArray, "/")

					fileNameMatched, _ := path.Match(magicString, node.Name())

					if (fileNameMatched) {
						if (len(rest) > 0 && GlobHasMagic(rest)) {
							pathsArray, _ = c.GetGlob(nextPath, pathsArray)
						} else {
							nextPathStat, _ = c.Stat(nextPath)
							pathsArray = append(pathsArray, nextPathStat)
						}
					}
				}
			} else if len(restElements) > 0 {
				pathsArray = append(pathsArray, fileInfo)
			}
		}
	}
	return pathsArray, nil
}
