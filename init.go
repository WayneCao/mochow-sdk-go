// init.go - just import the sub packages

// Package sdk imports all sub packages to build all of them when calling `go install', `go build'
// or `go get' commands.
package sdk

import (
	_ "github.com/baidu/mochow-sdk-go/v2/auth"       // register auth package
	_ "github.com/baidu/mochow-sdk-go/v2/client"     // register client package
	_ "github.com/baidu/mochow-sdk-go/v2/http"       // register http package
	_ "github.com/baidu/mochow-sdk-go/v2/mochow"     // register mochow package
	_ "github.com/baidu/mochow-sdk-go/v2/mochow/api" // register api package
	_ "github.com/baidu/mochow-sdk-go/v2/util"       // register util package
	_ "github.com/baidu/mochow-sdk-go/v2/util/log"   // register log package
)
