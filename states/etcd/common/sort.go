package common

import "sort"

type WithCollectionID interface {
	GetCollectionID() int64
}

// SortByCollection generic sort function
func SortByCollection[T WithCollectionID](items []T) {
	sort.Slice(items, func(i, j int) bool {
		return items[i].GetCollectionID() < items[j].GetCollectionID()
	})
}
