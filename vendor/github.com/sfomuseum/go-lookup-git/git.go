package git

import (
	"bytes"
	"context"
	"github.com/sfomuseum/go-lookup"
	gogit "gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/go-git.v4/storage/memory"
	"io/ioutil"
	"log"
	"net/url"
)

const DEFAULT_BRANCH string = "main"

type GitLookerUpper struct {
	lookup.LookerUpper
	uri string
	ref plumbing.ReferenceName
}

func NewGitLookerUpper(ctx context.Context) lookup.LookerUpper {

	ref := plumbing.NewBranchReferenceName(DEFAULT_BRANCH)

	l := &GitLookerUpper{
		ref: ref,
	}

	return l
}

func (l *GitLookerUpper) Open(ctx context.Context, uri string) error {

	u, err := url.Parse(uri)

	if err != nil {
		return err
	}

	q := u.Query()

	branch := q.Get("branch")

	if branch != "" {
		ref := plumbing.NewBranchReferenceName(branch)
		l.ref = ref
	}

	l.uri = uri

	return nil
}

func (l *GitLookerUpper) Append(ctx context.Context, lu lookup.Catalog, append_funcs ...lookup.AppendLookupFunc) error {

	r, err := gogit.Clone(memory.NewStorage(), nil, &gogit.CloneOptions{
		URL:   l.uri,
		Depth: 1,
	})

	if err != nil {
		return err
	}

	ref, err := r.Reference(l.ref, true)

	if err != nil {
		return err
	}

	commit, err := r.CommitObject(ref.Hash())

	if err != nil {
		return err
	}

	files, err := commit.Files()

	if err != nil {
		return err
	}

	err = files.ForEach(func(f *object.File) error {

		fh, err := f.Reader()

		if err != nil {
			return err
		}

		defer fh.Close()

		body, err := ioutil.ReadAll(fh)

		if err != nil {
			return err
		}

		for _, append_f := range append_funcs {

			br := bytes.NewReader(body)
			fh := ioutil.NopCloser(br)

			err := append_f(ctx, lu, fh)

			if err != nil {
				log.Printf("GIT %s: %s\n", f.Name, err)
				// return err
			}
		}

		return nil
	})

	return err
}
