package utils

import (
	"reflect"
)

// 1. Simple Overwrite Merge
func SimpleOverwriteMerge(a, b map[string]interface{}) map[string]interface{} {
	result := make(map[string]interface{})
	for k, v := range a {
		result[k] = v
	}
	for k, v := range b {
		result[k] = v
	}
	return result
}

// 2. Subgroup for Differences
func SubgroupDifferenceMerge(a, b map[string]interface{}) map[string]interface{} {
	result := make(map[string]interface{})
	for k, v := range a {
		if bv, exists := b[k]; exists && !reflect.DeepEqual(v, bv) {
			result[k] = map[string]interface{}{
				"a": v,
				"b": bv,
			}
		} else {
			result[k] = v
		}
	}
	for k, v := range b {
		if _, exists := a[k]; !exists {
			result[k] = v
		}
	}
	return result
}

// 3. List for Differences
func ListDifferenceMerge(a, b map[string]interface{}) (map[string]interface{}, error) {
	//var mapA, mapB map[string]interface{}
	result := make(map[string]interface{})

	// Unmarshal both json.RawMessage inputs into map[string]interface{}
	//if err := json.Unmarshal(a, &mapA); err != nil {
	//	return nil, err
	//}
	//if err := json.Unmarshal(b, &mapB); err != nil {
	//	return nil, err
	//}

	// Process mapA
	for k, v := range a {
		if bv, exists := b[k]; exists && !reflect.DeepEqual(v, bv) {
			result[k] = []interface{}{v, bv}
		} else {
			result[k] = v
		}
	}

	// Process mapB to add keys not in mapA
	for k, v := range b {
		if _, exists := a[k]; !exists {
			result[k] = v
		}
	}

	return result, nil
}

// 4. Deep Merge with Conflict Resolution
func DeepMergeWithResolution(a, b map[string]interface{}, resolver func(string, interface{}, interface{}) interface{}) map[string]interface{} {
	result := make(map[string]interface{})
	for k, v := range a {
		if bv, exists := b[k]; exists {
			result[k] = resolver(k, v, bv)
		} else {
			result[k] = v
		}
	}
	for k, v := range b {
		if _, exists := a[k]; !exists {
			result[k] = v
		}
	}
	return result
}
