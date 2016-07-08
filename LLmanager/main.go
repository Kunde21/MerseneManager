// Copyright ©2016 Chad Kunde. All rights reserved.
// Use and distribution of this source code is governed
// by an MIT-style license that can be found in the LICENSE file.

// Atomated manager for clLucas

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
	"time"

	"gopkg.in/yaml.v2"
)

// mersenne.org/manual_result limit is 2MB
// leave a 1K buffer for safety
const sendlimit = 2*1024*1024 - 1024

var (
	sett = settings{ // Default settings
		Usrname:  "",
		Pass:     "",
		Polltime: 12,
		Devices: []device{{
			Device:   0,
			Workdir:  ".",
			WorkType: 101,
			Cache:    2,
			GpuTh:    128},
		},
	}

	baseURL   *url.URL
	writeOpts bool
	jar, _    = cookiejar.New(nil) // cookiejar.New() doesn't have an error return path
	timeout   = 10 * time.Second   // http timeout

	workReg   = regexp.MustCompile(`(DoubleCheck|Test)=.*(,[0-9]+){3}`)
	resultReg = regexp.MustCompile(`M\( ([0-9]*) \).*`)
)

type settings struct {
	Usrname  string `yaml:"UserName"`
	Pass     string `yaml:"Password"`
	Polltime uint   `yaml:"Poll"`
	LogFile  string `yaml:"Logs"`
	poll     time.Duration
	Devices  []device `yaml:"Devices"`
}

type device struct {
	Device   uint   `yaml:"Device"`
	Workdir  string `yaml:"Directory"`
	WorkType uint   `yaml:"WorkType"`
	Cache    uint   `yaml:"Assignments"`
	GpuTh    uint   `yaml:"Threads"`
	files    fileSt
}

type fileSt struct {
	exec, todo, res, sent string
}

func init() {
	var err error
	baseURL, err = url.Parse("http://www.mersenne.org/")
	if err != nil {
		log.Fatal("Url Parse failure: ", err)
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
	for {
		if !login() {
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
			break
		}
		time.Sleep(sett.poll)
	}
}

func parseOpts() {
	flag.StringVar(&sett.Usrname, "usr", sett.Usrname, "REQUIRED: Primenet user name")
	flag.StringVar(&sett.Pass, "pass", sett.Pass, "REQUIRED: Primenet password")
	flag.UintVar(&sett.Polltime, "time", sett.Polltime, "Polling delay in hours, 0 to run once (max 120)")
	flag.UintVar(&sett.Devices[0].Device, "dev", sett.Devices[0].Device, "OpenCL device number for clLucas (default 0)")
	flag.UintVar(&sett.Devices[0].GpuTh, "threads", sett.Devices[0].GpuTh, "GPU threads")
	flag.UintVar(&sett.Devices[0].Cache, "n", sett.Devices[0].Cache, "Number of assignments to cache")
	flag.UintVar(&sett.Devices[0].WorkType, "T", sett.Devices[0].WorkType, "Worktype code: \n\t • 101: DC \n\t • 100: First-time LL \n\t • 102: WR LL\n\t")
	flag.StringVar(&sett.Devices[0].Workdir, "dir", sett.Devices[0].Workdir, `Work directory with worktodo.txt and results.txt`)
	flag.StringVar(&sett.LogFile, "logs", sett.LogFile, "Log file for LLmanager output")

	flag.BoolVar(&writeOpts, "w", false, "Write default settings to LLsettings.yml and exit")
	flag.Parse()

	if writeOpts {
		file, err := os.Create("LLsettings.yml")
		if err != nil {
			log.Fatalln("Error creating LLsettings.yml", err)
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

	if sett.Usrname == "" || sett.Pass == "" {
		flag.PrintDefaults()
		os.Exit(1)
	}

	if sett.Polltime > 120 {
		sett.Polltime = 120
	}
	sett.poll = time.Duration(sett.Polltime) * time.Hour
	log.SetFlags(log.LstdFlags | log.LUTC)
	log.SetPrefix("LLMgr: ")

	if sett.LogFile == "" {
		return
	}
	file, err := os.OpenFile(sett.LogFile, os.O_APPEND|os.O_CREATE, 0664)
	if err != nil {
		log.Fatalln("Error opening log file:", err)
	}
	log.SetOutput(file)
}

func parseYaml() {
	file, err := os.Open("LLsettings.yml")
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
		sent: filepath.FromSlash(dir + "/result_sent.txt"),
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
	work := getWork(dev.Cache-uint(len(curWrk)), dev.WorkType)
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

func getWork(n, workType uint) (work [][]byte) {
	log.Println("Getwork", n)
	asgnURL, err := baseURL.Parse("/manual_assignment/")
	if err != nil {
		log.Fatal("URL parse failure:", err)
	}
	reqV := asgnURL.Query()
	reqV.Set("cores", "1")
	reqV.Set("num_to_get", fmt.Sprint(n))
	reqV.Set("pref", fmt.Sprint(workType))
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
	return workReg.FindAll(body, -1)
}

func sendResults(dev device) (success bool) {
	if !lockFile(dev.files.res, dev.files.sent) {
		log.Println("SENDRESULT: Failed to lock files")
		return false
	}
	defer unlockFile(dev.files.res, dev.files.sent)

	res, err := os.OpenFile(dev.files.res, os.O_RDWR|os.O_CREATE, 0664)
	if err != nil {
		log.Println("SENDRESULT: Error opening results.txt", err)
	}
	defer res.Close()
	curr, err := ioutil.ReadAll(res)
	if err != nil {
		log.Println("SENDRESULT: Error reading results.txt", err)
		return false
	}
	curr = bytes.Replace(curr, []byte("\r"), []byte("\n"), -1)

	sent, err := os.OpenFile(dev.files.sent, os.O_APPEND|os.O_CREATE, 0664)
	if err != nil {
		log.Println("SENDRESULT: Error opening result_sent.txt", err)
		return false
	}
	defer sent.Close()

	curRes := resultReg.FindAll(curr, -1)
	if curRes == nil || len(curRes) == 0 {
		return true
	}
	results := bytes.TrimRight(bytes.Join(curRes, []byte("\n")), " \n")
	var loc int
	for i := 0; i < len(results)-1; i += loc {
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
			log.Println("SENDRESULT: result_sent.txt write error:", err)
			return false
		}
	}
	sent.Write([]byte("\n"))
	// All results sent successfully, clear results file
	res.Truncate(0)
	return true
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
