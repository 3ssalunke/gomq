package util

import (
	"math/rand"
	"strings"
	"unicode"
)

func SliceContains[T comparable](s []T, val T) bool {
	for _, v := range s {
		if v == val {
			return true
		}
	}
	return false
}

func MapContains[T comparable, S any](m map[T]S, k T) bool {
	if _, exists := m[k]; exists {
		return true
	}
	return false
}

func RemoveAllWhiteSpaces(s string) string {
	return strings.Map(func(r rune) rune {
		if unicode.IsSpace(r) {
			return -1
		}
		return r
	}, s)
}

func RemoveArrayElement[T comparable](s []T, index int) []T {
	return append(s[:index], s[index+1:]...)
}

func GenerateRandomInt() int {
	return rand.Intn(100)
}
