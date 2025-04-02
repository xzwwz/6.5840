package mr

import "fmt"
import "log"
import "os"
import "strconv"
import "strings"
import "bufio"
import "io/ioutil"


// var filepath string =  "./temp/task/"
var path string = "./files/"
var taskPath string = path+"task/"
var mapPath string = path+"map/" 
var reducePath string = path+"reduce/"

func createDirectory(dirPath string) error {
	// 创建空目录
	err := os.MkdirAll(dirPath, 0755)
	if err != nil {
		return fmt.Errorf("重新创建目录失败: %w", err)
	}

	return nil
}

func clearDirectory(dirPath string) error {
	// 删除整个目录（包括目录本身）
	err := os.RemoveAll(dirPath)
	if err != nil {
		return fmt.Errorf("删除目录失败: %w", err)
	}

	return nil
}

func splitfile(files []string) int {
    
    count := 0
    currentSize := 0
    var currentContent strings.Builder
    
    for _, filename := range files {
        file, err := os.Open(filename)
        if err != nil {
            fmt.Printf("cannot open file %v\n", filename)
            continue
        }
        defer file.Close()

        scanner := bufio.NewScanner(file)
        for scanner.Scan() {
            line := scanner.Text()
            lineSize := len(line) + 1 // +1 for newline character
            
            // If adding this line would exceed 64KB, write current content to file
            if currentSize + lineSize > 64*1024 && currentSize > 0 {
                if writefile(taskPath+"task-"+strconv.Itoa(count)+".txt", []byte(currentContent.String())) {
                    count++
                    currentContent.Reset()
                    currentSize = 0
                }
            }
            
            // Add the line to current content
            if currentSize > 0 {
                currentContent.WriteByte('\n')
            }
            currentContent.WriteString(line)
            currentSize += lineSize
        }
        
        if err := scanner.Err(); err != nil {
            fmt.Printf("error reading file %v: %v\n", filename, err)
            continue
        }
    }
    
    // Write any remaining content
    if currentSize > 0 {
        if writefile(taskPath+"task-"+strconv.Itoa(count)+".txt", []byte(currentContent.String())) {
            count++
        }
    }
    
    return count
}

func splitTask(files []string) []Task {
    tasks := []Task{}
    
    // var currentContent strings.Builder
    
    for _, filename := range files {
        file, err := os.Open(filename)
        if err != nil {
            fmt.Printf("cannot open file %v\n", filename)
            continue
        }
        defer file.Close()
        
        start := 0
        currentSize := 0
        scanner := bufio.NewScanner(file)

        for scanner.Scan() {
            line := scanner.Text()
            lineSize := len(line) + 1 // +1 for newline character
            
            // If adding this line would exceed 64KB, write current content to file
            if currentSize + lineSize > 1024*1024 && currentSize > 0 {
                // if writefile(taskPath+"task-"+strconv.Itoa(count)+".txt", []byte(currentContent.String())) {
                //     count++
                //     currentContent.Reset()
                //     currentSize = 0
                // }
                tasks = append(tasks,Task{
                    Filename:filename,
                    // state:0,
                    Start:start,
                    Size:currentSize,
                })
                start = start+currentSize
                currentSize = 0

            }
            
            currentSize += lineSize
        }

        if currentSize > 0 {
            tasks = append(tasks,Task{
                Filename:filename,
                // state:0,
                Start:start,
                Size:currentSize,
            })
        }
        
        if err := scanner.Err(); err != nil {
            fmt.Printf("error reading file %v: %v\n", filename, err)
            continue
        }
    }
    
    // Write any remaining content
    
    
    return tasks
}


func writefile(filename string, data []byte) bool {
	file, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		log.Print("cannot open file %v", filename)
		return false
	}
	defer file.Close()
	writer := bufio.NewWriter(file)
	_, err = writer.Write(data)
	if err != nil {
		log.Print("cannot write file %v", filename)
		return false
	}

	writer.Flush()
	file.Close()
	return true
}

func initfilesystem(){
    clearDirectory(path)
    createDirectory(taskPath)
    createDirectory(mapPath)
    createDirectory(reducePath)
}

func readfile(filename string) string {
    file, err := os.Open(filename)
	defer file.Close()
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
    return string(content)
}

func readTask(task Task) string{
    file,err:= os.Open(task.Filename)
    defer file.Close()
	if err != nil {
		log.Fatalf("cannot open %v", task.Filename)
	}
    buf := make([]byte,task.Size)
    _, err =file.ReadAt(buf,int64(task.Start))
    if err != nil {
		log.Fatalf("cannot read %v", task.Filename)
	}
    return string(buf)
}