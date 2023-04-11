package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/mail"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type MailDoc struct {
	ID       string    `json:"_id"`
	Username string    `json:"username"`
	Date     time.Time `json:"date"`
	Folder   string    `json:"folder"`
	Content  string    `json:"content"`
}

func main() {
	err := createIndex()
	if err != nil {
		log.Fatal(err)
	}

	err = filepath.Walk(fmt.Sprintf("%v/%v/maildir", os.Args[1], os.Args[2]), func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}

		// Read file content
		content, err := ioutil.ReadFile(path)
		if err != nil {
			return err
		}

		paths := strings.Split(path, "\\")
		if len(paths) >= 5 {
			postOnIndex(path, content)
		} else {
			fmt.Println("Not uploaded: ", paths)
		}

		return nil
	})

	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Indexing complete")
}

func createIndex() error {
	// This creates the index if it doesn't exist
	client := &http.Client{}
	headUrl := "http://localhost:4080/api/index/mailsIndex"
	headr, err := http.NewRequest("HEAD", headUrl, nil)
	if err != nil {
		log.Fatal(err)
	}
	headr.SetBasicAuth("admin", "Complexpass#123")
	res, err := client.Do(headr)
	if err != nil {
		log.Fatal(err)
	}

	if res.StatusCode == http.StatusNotFound {
		postUrl := "http://localhost:4080/api/index"
		newIndex := []byte(`{
			"name": "mailsIndex",
			"mappings": {
				"properties": {
					"username": {
						"type": "text",
						"index": true,
						"store": true,
						"highlightable": true
					},
					"content": {
						"type": "text",
						"index": true,
						"store": true,
						"highlightable": true
					},
					"date": {
						"type": "keyword",
						"format": "2006-01-02T15:04:05Z-05:00",
						"index": true,
						"sortable": true,
						"aggregatable": true
					},
					"folder": {
						"type": "text",
						"index": true,
						"store": true,
						"highlightable": true
					}
				}
			} 
		}`)

		r, err := http.NewRequest("POST", postUrl, bytes.NewBuffer(newIndex))
		if err != nil {
			log.Fatal(err)
		}

		r.SetBasicAuth("admin", "Complexpass#123")
		_, err = client.Do(r)

		if err != nil {
			log.Fatal(err)
		}
	}

	return nil
}

func findExistingDocs(getUrl string) bool {
	client := &http.Client{}
	getRequest, _ := http.NewRequest("GET", getUrl, nil)
	getRequest.SetBasicAuth("admin", "Complexpass#123")
	getRes, getErr := client.Do(getRequest)
	time.Sleep(10)
	if getErr != nil {
		log.Fatal(getErr)
	}
	defer getRes.Body.Close()
	getResBody, _ := io.ReadAll(getRes.Body)

	if strings.Compare(string(getResBody), `{"error":"id not found"}`) == 0 {
		return true
	}
	return false
}

func postOnIndex(path string, content []byte) error {
	// Create document ID
	paths := strings.Split(path, "\\")
	docID := strings.Replace(path, "\\", "_", -1)
	docID = strings.Replace(docID, ".", "_", -1)

	baseUrl := "http://localhost:4080/api/"
	getUrl := baseUrl + "mailsIndex/_doc/" + docID
	postUrl := baseUrl + "mailsIndex/_doc"
	client := &http.Client{}

	// check if current Id already exists
	// if nothing was found, upload new
	isDocIndexed := findExistingDocs(getUrl)

	if isDocIndexed {
		contentAsString := string(content)
		contentAsString = strings.ReplaceAll(contentAsString, "\"", "\\\"")
		contentAsMail, err := mail.ReadMessage(strings.NewReader(contentAsString))

		if err != nil {
			fmt.Println("Failing file at mail.readMessage: ", paths)
			return nil
			// log.Fatal(err)
		}
		mailBody, err := io.ReadAll(contentAsMail.Body)
		if err != nil {
			fmt.Println("Failing file at io.ReadAll: ", paths)
			fmt.Println(err)
			return nil
			// log.Fatal(err)
		}
		mailDate, err := mail.ParseDate(contentAsMail.Header.Get("Date"))
		if err != nil {
			fmt.Println("Failing file at mail.ParseDate: ", paths)
			return nil
			// log.Fatal(err)
		}

		routeToFile := strings.Join(paths[3:len(paths)-2], "/")

		mailPosting := &MailDoc{
			ID:       docID,
			Username: paths[2],
			Content:  string(mailBody),
			Folder:   routeToFile,
			Date:     mailDate,
		}
		jsonMailPosting, _ := json.Marshal(mailPosting)
		postRequest, err := http.NewRequest("POST", postUrl, bytes.NewBuffer(jsonMailPosting))
		postRequest.SetBasicAuth("admin", "Complexpass#123")

		postRes, postErr := client.Do(postRequest)
		time.Sleep(10)
		if postErr != nil {
			log.Fatal(postErr)
		}
		if postRes.StatusCode != http.StatusOK {
			fmt.Println(paths)
		}
		defer postRes.Body.Close()
	}

	return nil
}
