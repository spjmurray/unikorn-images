package main

import (
	"context"
	"fmt"
	"maps"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/gophercloud/gophercloud/v2/openstack"
	"github.com/gophercloud/gophercloud/v2/openstack/config"
	"github.com/gophercloud/gophercloud/v2/openstack/config/clouds"
	"github.com/gophercloud/gophercloud/v2/openstack/image/v2/images"
	"github.com/kaptinlin/jsonschema"
)

const schemav2 = `{
	"type":"object",
	"required":[
	  "unikorn:os:kernel",
	  "unikorn:os:family",
	  "unikorn:os:distro",
	  "unikorn:os:version",
	  "unikorn:virtualization"
	],
	"properties":{
		"unikorn:os:kernel":{
			"type":"string",
			"enum":[
				"linux"
			]
		},
		"unikorn:os:family":{
                        "type":"string",
			"enum":[
				"debian",
				"redhat"
			]
                },
		"unikorn:os:distro":{
                        "type":"string",
			"enum":[
				"ubuntu",
				"rocky"
			]
                },
		"unikorn:os:variant":{
                        "type":"string"
                },
		"unikorn:os:codename":{
                        "type":"string"
                },
		"unikorn:os:version":{
                        "type":"string"
                },
		"unikorn:package:kubernetes":{
                        "type":"string"
                },
		"unikorn:package:slurmd":{
                        "type":"string"
                },
		"unikorn:gpu_vendor":{
                        "type":"string",
			"enum":[
				"AMD",
				"NVIDIA"
			]
                },
		"unikorn:gpu_models":{
                        "type":"string"
                },
		"unikorn:gpu_driver":{
                        "type":"string"
                },
		"unikorn:virtualization":{
                        "type":"string",
			"enum":[
				"any",
				"baremetal",
				"virtualized"
			]
                },
		"unikorn:digest":{
                        "type":"string"
                }
	}
}`

func process(image *images.Image, schema *jsonschema.Schema) {
	fmt.Println("---")
	fmt.Println("id:", image.ID)
	fmt.Println("name:", image.Name)
	fmt.Println("createdAt:", image.CreatedAt)
	fmt.Println("sizeGiB:", image.SizeBytes>>30)

	result := schema.Validate(image.Properties)
	if !result.Valid {
		fmt.Println("error:")
		fmt.Println("  message: [1;31mImage does not match Unikorn Schema V2[0m")
		fmt.Println("  documentation: See https://github.com/unikorn-cloud/specifications/blob/main/specifications/providers/openstack/flavors_and_images.md")
		fmt.Println("  detail:")

		for errorType := range maps.Keys(result.Errors) {
			evaluationError := result.Errors[errorType]

			switch errorType {
			case "properties":
				fmt.Println("  - message: Object properties failed validation or do not exist")
				// It may either be pluralized or not...
				for _, k := range []string{"property", "properties"} {
					if v, ok := evaluationError.Params[k]; ok {
						fmt.Println("    properties:", v)
					}
				}
			}
		}

		return
	}

	fmt.Println("os:")

	for _, name := range []string{"kernel", "family", "distro", "variant", "codename", "version"} {
		fmt.Println("  "+name+":", image.Properties["unikorn:os:"+name])
	}

	fmt.Println("package:")

	for _, name := range []string{"kubernetes", "slurmd"} {
		fmt.Println("  "+name+":", image.Properties["unikorn:package:"+name])
	}

	fmt.Println("gpu:")

	for _, name := range []string{"vendor", "models", "driver"} {
		fmt.Println("  "+name+":", image.Properties["unikorn:gpu_"+name])
	}

	fmt.Println("virtualization:", image.Properties["unikorn:virtualization"])
	fmt.Println("digest:", image.Properties["unikorn:digest"])
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	compiler := jsonschema.NewCompiler()

	schema, err := compiler.Compile([]byte(schemav2))
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	authOptions, endpointOpts, tlsConfig, err := clouds.Parse()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	providerClient, err := config.NewProviderClient(ctx, authOptions, config.WithTLSConfig(tlsConfig))
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	client, err := openstack.NewImageV2(providerClient, endpointOpts)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	opts := &images.ListOpts{
		Visibility: images.ImageVisibilityPublic,
	}

	page, err := images.List(client, opts).AllPages(ctx)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	result, err := images.ExtractImages(page)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	result = slices.DeleteFunc(result, func(image images.Image) bool {
		return !slices.ContainsFunc(slices.Collect(maps.Keys(image.Properties)), func(key string) bool {
			return strings.HasPrefix(key, "unikorn:")
		})
	})

	for i := range result {
		process(&result[i], schema)
	}
}
