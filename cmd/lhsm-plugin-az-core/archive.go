package lhsm_plugin_az_core

import (
	"bytes"
	"context"
	"fmt"
	"net/url"
	"os"
	"path"
	"strings"
	"syscall"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/wastore/lemur/cmd/util"
)

type ArchiveOptions struct {
	AccountName   string
	ContainerName string
	ResourceSAS   string
	MountRoot     string
	BlobName      string
	SourcePath    string
	Credential    azblob.Credential
	Parallelism   uint16
	BlockSize     int64
	Pacer         util.Pacer
	ExportPrefix  string
	HNSEnabled    bool
}

// persist a blob to the local filesystem
func Archive(o ArchiveOptions) (int64, error) {
	archiveCtx := context.Background()
	ctx, cancel := context.WithCancel(archiveCtx)
	defer cancel()

	p := util.NewPipeline(ctx, o.Credential, o.Pacer, azblob.PipelineOptions{})

	//Get the blob URL
	cURL, _ := url.Parse(fmt.Sprintf("https://%s.blob.core.windows.net/%s%s", o.AccountName, o.ContainerName, o.ResourceSAS))
	containerURL := azblob.NewContainerURL(*cURL, p)
	blobURL := containerURL.NewBlockBlobURL(o.BlobName)

	util.Log(pipeline.LogInfo, fmt.Sprintf("Archiving %s", o.BlobName))

	//1. Upload and perserve permissions and acl for parents
	parents := strings.Split(o.BlobName, string(os.PathSeparator))
	parents = parents[:len(parents)-1]
	u := cURL
	dirPath := o.MountRoot

	for _, currDir := range parents {
		var acl string
		u.Path = path.Join(u.Path, currDir) //keep appending path to the url
		dirURL := azblob.NewBlockBlobURL(*u, p)
		meta := azblob.Metadata{}

		//Get owner, group and perms
		dirPath = path.Join(dirPath, currDir)
		dir, err := os.Open(dirPath)
		if err != nil {
			util.Log(pipeline.LogError, fmt.Sprintf("Archiving %s. Failed to get Access Control: %s", o.BlobName, err.Error()))
			return 0, err
		}
		defer dir.Close()
		dirInfo, err := dir.Stat()
		if err != nil {
			util.Log(pipeline.LogError, fmt.Sprintf("Archiving %s. Failed to get Access Control: %s", o.BlobName, err.Error()))
			return 0, err
		}
		owner := fmt.Sprintf("%d", dirInfo.Sys().(*syscall.Stat_t).Uid)
		permissions := fmt.Sprintf("%o", dirInfo.Mode())
		group := fmt.Sprintf("%d", dirInfo.Sys().(*syscall.Stat_t).Gid)
		modTime := dirInfo.ModTime().Format("2006-01-02 15:04:05 -0700")

		if o.HNSEnabled {
			aclResp, err := dirURL.GetAccessControl(ctx, nil, nil, nil, nil, nil, nil, nil, nil)
			if stgErr, ok := err.(azblob.StorageError); err != nil || ok && stgErr.ServiceCode() == azblob.ServiceCodeBlobNotFound {
				util.Log(pipeline.LogError, fmt.Sprintf("Archiving %s. Failed to get Access Control: %s", o.BlobName, err.Error()))
				return 0, err
			}
			acl = aclResp.XMsACL()
		}

		meta["hdi_isfolder"] = "true"
		if !o.HNSEnabled {
			meta["Permissions"] = permissions
			meta["ModTime"] = modTime
			meta["Owner"] = owner
			meta["Group"] = group
		}

		_, err = dirURL.Upload(ctx, bytes.NewReader(nil), azblob.BlobHTTPHeaders{}, meta, azblob.BlobAccessConditions{}, azblob.AccessTierNone, azblob.BlobTagsMap{}, azblob.ClientProvidedKeyOptions{})

		if err != nil {
			util.Log(pipeline.LogError, fmt.Sprintf("Archiving %s. Failed to upload directory: %s", u.Path, err.Error()))
			return 0, err
		}

		if o.HNSEnabled {
			_, err := dirURL.SetAccessControl(ctx, nil, nil, nil, nil, nil, &acl, nil, nil, nil, nil, nil)
			if err != nil {
				util.Log(pipeline.LogError, fmt.Sprintf("Archiving %s. Failed to get Access Control: %s", u.Path, err.Error()))
				return 0, err
			}
		}
	}

	// 2. Upload the file
	// open the file to read from
	file, _ := os.Open(o.SourcePath)
	fileInfo, _ := file.Stat()
	defer file.Close()

	//Save owner, perm, group and acl info
	total := fileInfo.Size()
	meta := azblob.Metadata{}
	owner := fmt.Sprintf("%d", fileInfo.Sys().(*syscall.Stat_t).Uid)
	permissions := fmt.Sprintf("%o", fileInfo.Mode())
	group := fmt.Sprintf("%d", fileInfo.Sys().(*syscall.Stat_t).Gid)
	modTime := fileInfo.ModTime().Format("2006-01-02 15:04:05 -0700")
	var acl string

	meta["Permissions"] = permissions
	meta["ModTime"] = modTime
	meta["Owner"] = owner
	meta["Group"] = group

	if o.HNSEnabled {
		u, _ := url.Parse(fmt.Sprintf("https://%s.dfs.core.windows.net/%s/%s%s", o.AccountName, o.ContainerName, o.BlobName, o.ResourceSAS))
		dfsURL := azblob.NewBlockBlobURL(*u, p)
		aclResp, err := dfsURL.GetAccessControl(ctx, nil, nil, nil, nil, nil, nil, nil, nil)
		if err != nil {
			util.Log(pipeline.LogError, fmt.Sprintf("Archiving %s. Failed to get Access Control: %s", o.BlobName, err.Error()))
			return 0, err
		}
		acl = aclResp.XMsACL()
	}

	_, err := azblob.UploadFileToBlockBlob(
		ctx,
		file,
		blobURL,
		azblob.UploadToBlockBlobOptions{
			BlockSize:   o.BlockSize,
			Parallelism: o.Parallelism,
			Metadata:    meta,
		})

	if err != nil {
		util.Log(pipeline.LogError, fmt.Sprintf("Archiving %s. Failed to upload blob: %s", o.BlobName, err.Error()))
		return 0, err
	}

	if o.HNSEnabled {
		u, _ := url.Parse(fmt.Sprintf("https://%s.dfs.core.windows.net/%s/%s%s", o.AccountName, o.ContainerName, o.BlobName, o.ResourceSAS))
		dfsURL := azblob.NewBlockBlobURL(*u, p)
		/*
			_, err = dfsURL.SetAccessControl(ctx, nil, nil, &owner, &group, &permissions, nil, nil, nil, nil, nil, nil)
			if err != nil {
				util.Log(pipeline.LogError, fmt.Sprintf("Archiving %s, Failed to set owner, group and permissions: %s", o.BlobName, err.Error()))
				return 0, err
			}
		*/
		_, err = dfsURL.SetAccessControl(ctx, nil, nil, nil, nil, nil, &acl, nil, nil, nil, nil, nil)
		if err != nil {
			util.Log(pipeline.LogError, fmt.Sprintf("Archiving %s. Failed to set AccessControl: %s", o.BlobName, err.Error()))
			//TODO: should we delete blob?
		}
	}

	return total, err
}
