package lookup

import (
	"context"
	"log"
)

func SeedCatalog(ctx context.Context, c Catalog, looker_uppers []LookerUpper, append_funcs []AppendLookupFunc) error {

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	done_ch := make(chan bool)
	err_ch := make(chan error)

	remaining := len(looker_uppers)

	for _, l := range looker_uppers {

		go func(l LookerUpper) {

			log.Printf("APPEND %v -> %T\n", c, l)
			err := l.Append(ctx, c, append_funcs...)

			if err != nil {
				err_ch <- err
			}

			done_ch <- true

		}(l)
	}

	for remaining > 0 {
		select {
		case <-done_ch:
			remaining -= 1
		case err := <-err_ch:
			return err
		default:
			// pass
		}
	}

	return nil
}
