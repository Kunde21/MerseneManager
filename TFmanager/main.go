// Copyright ©2016 Chad Kunde. All rights reserved.
// Use and distribution of this source code is governed
// by an MIT-style license that can be found in the LICENSE file.

// Automated Manager for mfakto

package main

import (
	"bytes"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"time"

	"gopkg.in/yaml.v2"
)

// mersenne.org/manual_result limit is 2MB
// leave a 1K buffer for safety
const sendlimit = 2*1024*1024 - 1024

var (
	sett = settings{ // Default settings
		Polltime: 2,
		Devices: []device{{
			Device:     0,
			Workdir:    ".",
			WorkType:   "lltf",
			WorkOption: "what_makes_sense",
			Target:     73,
			Cache:      5},
		},
	}

	baseURL, gpu72URL *url.URL
	writeOpts         bool
	jar, _            = cookiejar.New(nil) // cookiejar.New() doesn't have an error return path
	timeout           = 30 * time.Second   // http timeout

	workReg       = regexp.MustCompile(`(Factor)=.*(,[0-9]+){3}`)
	resultReg     = regexp.MustCompile(`.*M([0-9]+) .*`)
	resultExtract = regexp.MustCompile(`M([0-9]+)`)
)

type settings struct {
	Usrname   string `yaml:"UserName"`
	Pass      string `yaml:"Password"`
	GPU72Usr  string `yaml:"GPU72UserName"`
	GPU72Pass string `yaml:"GPU72Password"`
	Polltime  uint   `yaml:"Poll"`
	LogFile   string `yaml:"Logs"`
	poll      time.Duration
	primenet  bool
	gpu72     bool
	Devices   []device `yaml:"Devices"`
}

type device struct {
	Device     uint   `yaml:"Device"`
	Workdir    string `yaml:"Directory"`
	WorkType   string `yaml:"WorkType"`
	WorkOption string `yaml:"WorkOption"`
	gpu72Opt   uint
	Target     uint `yaml:"TargetExponent"`
	Cache      uint `yaml:"Assignments"`
	files      fileSt
}

type fileSt struct {
	exec, todo, res, sent string
}

func init() {
	var err error
	baseURL, err = url.Parse("http://www.mersenne.org/")
	if err != nil {
		log.Fatal("Primenet url Parse failure: ", err)
	}
	gpu72URL, err = url.Parse("http://www.gpu72.com/")
	if err != nil {
		log.Fatal("GPU72 url Parse failure: ", err)
	}
	parseYaml()                   // Parse the settings.yml file
	parseOpts()                   // Parse cmd line args (override yaml)
	for i := range sett.Devices { // Fill out the file struct
		getFiles(&sett.Devices[i])
	}
}

func main() {
	if writeOpts {
		return
	}

polling:
	for ct := 0; ct < 10; ct++ { // loop counter is used as a retry counter
		if !login() && !sett.gpu72 {
			log.Println("Login retry in 2 minutes")
			time.Sleep(2 * time.Minute)
			continue
		}
		for i := range sett.Devices {
			log.Println("Updating device: ", i)
			if !(topoff(sett.Devices[i]) && sendResults(sett.Devices[i])) {
				log.Println("Update failed, retry in 2 minutes")
				time.Sleep(2 * time.Minute)
				continue polling
			}
		}
		log.Println("Update Complete")
		if sett.Polltime == 0 {
			fmt.Println("Exiting")
			break
		}
		time.Sleep(sett.poll)
		ct = -1 // Zero out the retry counter
	}
	log.Fatal("Failed 10 update attempts, exiting.")
}

func parseOpts() {
	flag.StringVar(&sett.Usrname, "usr", sett.Usrname, "REQUIRED: Primenet user name")
	flag.StringVar(&sett.Pass, "pass", sett.Pass, "REQUIRED: Primenet password")
	flag.StringVar(&sett.GPU72Usr, "gusr", sett.GPU72Usr, "GPU72 user name")
	flag.StringVar(&sett.GPU72Pass, "gpass", sett.GPU72Pass, "GPU72 password")
	flag.UintVar(&sett.Polltime, "time", sett.Polltime, "Polling delay in hours, 0 to run once (max 120)")
	flag.UintVar(&sett.Devices[0].Device, "dev", sett.Devices[0].Device, "OpenCL device number for clLucas (default 0)")
	flag.UintVar(&sett.Devices[0].Cache, "n", sett.Devices[0].Cache, "Number of assignments to cache")
	flag.UintVar(&sett.Devices[0].Target, "tgt", sett.Devices[0].Target, `Target "Will factor to" exponenet (minimum 73)`)
	flag.StringVar(&sett.Devices[0].WorkType, "T", sett.Devices[0].WorkType, "Worktype code: lltf or dctf")
	flag.StringVar(&sett.Devices[0].WorkOption, "opt", sett.Devices[0].WorkOption, `Work Options: 
	• what_makes_sense 
	• lowest_tf_level 
	• highest_tf_level
	• lowest_exponent 
	• oldest_exponent 
	• let_gpu72_decide
	`)
	flag.StringVar(&sett.Devices[0].Workdir, "dir", sett.Devices[0].Workdir, `Work directory with worktodo.txt and results.txt`)
	flag.StringVar(&sett.LogFile, "logs", sett.LogFile, "Log file for LLmanager output")

	flag.BoolVar(&writeOpts, "w", false, "Write default settings to TFsettings.yml and exit")
	flag.Parse()

	if writeOpts {
		file, err := os.Create("TFsettings.yml")
		if err != nil {
			log.Fatalln("Error creating TFsettings.yml", err)
		}
		defer file.Close()
		st, err := yaml.Marshal(&sett)
		if err != nil {
			log.Println("Settings marshal error")
		}
		n, err := file.Write(st)
		if n != len(st) || err != nil {
			log.Printf("Write error %b of %b bytes: %v\n", n, len(st), err)
		}
		return
	}

	sett.gpu72 = (sett.GPU72Usr != "" && sett.GPU72Pass != "")
	sett.primenet = (sett.Usrname != "" && sett.Pass != "")

	if !(sett.primenet || sett.gpu72) {
		flag.PrintDefaults()
		os.Exit(1)
	}

	if sett.Polltime > 120 {
		sett.Polltime = 120
	}
	sett.poll = time.Duration(sett.Polltime) * time.Hour
	log.SetFlags(log.LstdFlags | log.LUTC)
	log.SetPrefix("TFMgr: ")

	if sett.LogFile == "" {
		return
	}
	file, err := os.OpenFile(sett.LogFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0664)
	if err != nil {
		log.Fatalln("Error opening log file:", err)
	}
	log.SetOutput(file)
}

func parseYaml() {
	file, err := os.Open("TFsettings.yml")
	if err != nil {
		return
	}
	defer file.Close()
	contents, err := ioutil.ReadAll(file)
	if err != nil {
		log.Println("Yaml file read failure:", err)
		return
	}
	err = yaml.Unmarshal(contents, &sett)
	if err != nil {
		log.Println("Yaml unmarshal error:", err)
	}
}

func getFiles(dev *device) {
	dir, err := filepath.Abs(dev.Workdir)
	if err != nil {
		log.Fatal("Workdir path cannot be resolved:", dev.Workdir)
	}
	dev.files = fileSt{
		todo: filepath.FromSlash(dir + "/worktodo.txt"),
		res:  filepath.FromSlash(dir + "/results.txt"),
		sent: filepath.FromSlash(dir + "/results_sent.txt"),
	}
	if dev.WorkType != "dctf" {
		dev.WorkType = "lltf"
	}

	switch dev.WorkOption {
	case "lowest_tf_level":
		dev.gpu72Opt = 1
	case "highest_tf_level":
		dev.gpu72Opt = 2
	case "lowest_exponent":
		dev.gpu72Opt = 3
	case "oldest_exponent":
		dev.gpu72Opt = 4
	case "let_gpu72_decide":
		dev.gpu72Opt = 9
	default:
		dev.gpu72Opt = 0
	}

	if dev.Target < 73 {
		dev.Target = 73
	}
}

func login() (loggedin bool) {
	login := url.Values{}
	login.Set("user_login", sett.Usrname)
	login.Set("user_password", sett.Pass)

	call := http.Client{Transport: nil, CheckRedirect: nil, Jar: jar, Timeout: timeout}
	resp, err := call.PostForm(baseURL.String(), login)
	if err != nil {
		log.Println("Primenet login failed: ", err)
		return false
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Println("Primenet login response read error: ", err)
		return false
	}
	if bytes.Contains(body, []byte(sett.Usrname+`<br>logged in`)) {
		return true
	}
	return false
}

func topoff(dev device) (success bool) {
	if !lockFile(dev.files.todo) {
		log.Println("Error locking worktodo.txt")
		return false
	}
	defer unlockFile(dev.files.todo)
	todo, err := os.OpenFile(dev.files.todo, os.O_RDWR|os.O_CREATE, 0664)
	if err != nil {
		log.Println("Error opening", dev.files.todo, err)
		return false
	}
	defer todo.Close()
	curr, err := ioutil.ReadAll(todo)
	if err != nil {
		log.Println("Error reading", dev.files.todo, err)
		return false
	}
	curr = bytes.Replace(curr, []byte("\r"), []byte("\n"), -1)

	curWrk := workReg.FindAll(curr, -1)
	if curWrk == nil {
		curWrk = make([][]byte, 0)
	}
	if len(curWrk) >= int(dev.Cache) {
		return true
	}
	log.Println("Getwork", dev.Cache-uint(len(curWrk)))
	var work [][]byte
	if sett.gpu72 {
		work = getWorkGPU72(dev.Cache-uint(len(curWrk)), dev)
	}
	if len(work) == 0 && sett.primenet { // This will catch !sett.gpu72 and getWorkGPU72 failures
		work = getWork(dev.Cache-uint(len(curWrk)), dev)
	}
	if work == nil {
		log.Println("No new work fetched")
		return false
	}
	work = append(curWrk, work...)

	workFile := bytes.Join(work, []byte("\n"))
	todo.Truncate(0)
	n, err := todo.WriteAt(workFile, 0)
	if err != nil || n != len(workFile) {
		log.Println("worktodo.txt write error:", err)
		log.Println(string(workFile))
		return false
	}
	return true
}

func getWorkGPU72(n uint, dev device) (work [][]byte) {
	asgnURL, err := gpu72URL.Parse(fmt.Sprintf("/account/getassignments/%s/", dev.WorkType))
	if err != nil {
		log.Fatal("URL parse failure:", err)
	}
	reqV := asgnURL.Query()
	reqV.Set("Number", fmt.Sprint(n))
	reqV.Set("GHzDays", "")
	reqV.Set("Low", "")
	reqV.Set("High", "")
	reqV.Set("Pledge", fmt.Sprint(dev.Target))

	req, err := http.NewRequest(http.MethodPost, asgnURL.String(), bytes.NewBufferString(reqV.Encode()))
	if err != nil {
		log.Println("Error creating gpu72 request", err)
		return nil
	}
	req.SetBasicAuth(sett.GPU72Usr, sett.GPU72Pass)
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Add("Content-Length", fmt.Sprint(len(reqV.Encode())))

	call := http.Client{Transport: nil, CheckRedirect: nil, Jar: jar, Timeout: timeout}
	resp, err := call.Do(req)
	if err != nil {
		log.Println("Connection Error", err)
		return nil
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Println("Reading response body failed", err)
		return nil
	}
	w := workReg.FindAll(body, -1)
	w2 := make([][]byte, 0, len(w))

	// GPU72 gives each assignment twice in the page source
	// so we need to filter it down to unique assignments
filterDups:
	for i := range w {
		for j := range w2 {
			if bytes.Compare(w[i], w2[j]) == 0 {
				continue filterDups
			}
		}
		w2 = append(w2, w[i])
	}
	return w2
}

func getWork(n uint, dev device) (work [][]byte) {
	asgnURL, err := baseURL.Parse("/manual_assignment/")
	if err != nil {
		log.Fatal("URL parse failure:", err)
	}
	reqV := asgnURL.Query()
	reqV.Set("cores", "1")
	reqV.Set("num_to_get", fmt.Sprint(n))
	reqV.Set("pref", "2") // Trial Factoring is code "2"
	reqV.Set("exp_lo", "")
	reqV.Set("exp_hi", "")
	reqV.Set("B1", "Get Assignments")
	asgnURL.RawQuery = reqV.Encode()

	call := http.Client{Transport: nil, CheckRedirect: nil, Jar: jar, Timeout: timeout}
	resp, err := call.Get(asgnURL.String())
	if err != nil {
		log.Printf("Connection Error: %v", err)
		return nil
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Println("Reading response body failed ", err)
		return nil
	}
	wrk := workReg.FindAll(body, -1)
	return setTargets(dev, wrk)
}

func setTargets(dev device, wrk [][]byte) [][]byte {
	target := []byte(fmt.Sprint(dev.Target))
	for i := range wrk {
		idx := bytes.LastIndex(wrk[i], []byte(","))
		val, _ := strconv.Atoi(string(wrk[i][idx+1:])) //Regex ensures this can only be digits
		if uint(val) < dev.Target {
			copy(wrk[i][idx+1:], target)
		}
	}
	return wrk
}

func sendResults(dev device) (success bool) {
	// Lock files
	if !lockFile(dev.files.res, dev.files.sent, dev.files.todo) {
		log.Println("Failed to lock results.txt")
		return false
	}
	defer unlockFile(dev.files.res, dev.files.sent, dev.files.todo)

	// Open files
	todo, err := os.OpenFile(dev.files.todo, os.O_RDWR, 0664)
	if err != nil {
		log.Println("SENDRESULT: Error opening worktodo.txt", err)
		return false
	}
	defer todo.Close()
	res, err := os.OpenFile(dev.files.res, os.O_RDWR|os.O_CREATE, 0664)
	if err != nil {
		log.Println("GETWORK: Error opening results.txt", err)
		return false
	}
	defer res.Close()
	sent, err := os.OpenFile(dev.files.sent, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0664)
	if err != nil {
		log.Println("GETWORK: Error opening result_sent.txt", err)
		return false
	}
	defer sent.Close()

	// read in worktodo and results
	asgn, err := ioutil.ReadAll(todo)
	if err != nil {
		log.Println("SENDRESULT: Error reading worktodo.txt", err)
		return false
	}
	asgn = bytes.Replace(asgn, []byte("\r"), []byte("\n"), -1)
	curr, err := ioutil.ReadAll(res)
	if err != nil {
		log.Println("GETWORK: Error reading results.txt", err)
		return false
	}
	curr = bytes.Replace(curr, []byte("\r"), []byte("\n"), -1)

	// Parse Result lines
	curRes := resultReg.FindAll(curr, -1)
	if curRes == nil || len(curRes) == 0 {
		return true
	}

	keep, send := filterResults(curRes, asgn)

	log.Println("Results:", len(keep)+len(send), "Sending Completed:", len(send))

	results := bytes.TrimRight(bytes.Join(send, []byte("\n")), " \n")
	for loc, i := 0, 0; i < len(results)-1; i += loc {
		if len(results[i:]) > sendlimit {
			loc = bytes.LastIndexByte(results[i:i+sendlimit], '\n') + 1
		} else {
			loc = len(results[i:])
		}
		// Protect against junk data in results file
		if loc <= 0 {
			log.Println("Loc error", loc, i, len(results[i:]))
			return false
		}
		if !sendbatch(results[i : i+loc]) {
			log.Println("SendBatch Failed", i, loc, results[i:i+loc])
			return false
		}
		n, err := sent.Write(results[i : i+loc])
		if err != nil || n != len(results[i:i+loc]) {
			log.Println("GETWORK: result_sent.txt write error:", err)
			return false
		}
	}
	if len(send) > 0 {
		sent.Write([]byte("\n"))
	}

	keepRes := append(bytes.Join(keep, []byte("\n")), byte('\n'))
	n, err := res.WriteAt(keepRes, 0)
	if n == len(keepRes) && err == nil {
		res.Truncate(int64(n))
	} else {
		log.Printf("Write error of kept results: %d of %d  err: %v\n", n, len(keepRes), err)
	}

	return true
}

func filterResults(results [][]byte, todo []byte) (keep, send [][]byte) {
	asgns := make(map[string]int)
	keep = make([][]byte, 0, len(results))
	send = make([][]byte, 0, len(results))
	for i := range results {
		asgn := resultExtract.Find(results[i])
		if loc, ok := asgns[string(asgn)]; ok {
			switch loc {
			case 1:
				keep = append(keep, results[i])
			case 2:
				send = append(send, results[i])
			}
		} else {
			switch bytes.Contains(todo, asgn[1:]) {
			case true:
				asgns[string(asgn)] = 1
				keep = append(keep, results[i])
			case false:
				asgns[string(asgn)] = 2
				send = append(send, results[i])
			}
		}
	}
	return keep, send
}

func sendbatch(batch []byte) (success bool) {
	sendURL, err := baseURL.Parse("/manual_result/default.php")
	if err != nil {
		log.Fatal("SENDBATCH: URL parse failure:", err)
	}
	reqV := sendURL.Query()
	reqV.Set("data", string(batch))
	reqV.Set("B1", "Submit")

	call := http.Client{Transport: nil, CheckRedirect: nil, Jar: jar, Timeout: timeout}
	resp, err := call.PostForm(sendURL.String(), reqV)
	if err != nil {
		log.Println("SENDBATCH: Connection Error", err)
		return false
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Println("SENDBATCH:  Primenet login response read error: ", err)
		return false
	}
	if bytes.Contains(body, []byte("processing:")) {
		return true
	}
	return false
}

func lockFile(fnames ...string) (locked bool) {
	var f *os.File
	var err error
	for j, fname := range fnames {
		// retry loop in case the file is locked
		for i := 0; i < 5; i++ {
			f, err = os.OpenFile(fname+".lck", os.O_CREATE|os.O_EXCL, 0660)
			if err == nil {
				f.Close()
				break
			}
			time.Sleep(5 * time.Second)
		}
		if err != nil {
			// Failure path, unlock all locked files before returning
			unlockFile(fnames[:j]...)
			return false
		}
	}
	return true
}

func unlockFile(fnames ...string) {
	var err error
	for _, fname := range fnames {
		// retry loop for safety
		for i := 0; i < 5; i++ {
			err = os.Remove(fname + ".lck")
			if err == nil {
				break
			}
			time.Sleep(5 * time.Second)
		}
	}
}
