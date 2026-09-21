package service

import "strconv"

// parsePositive 解析正整数查询参数。
func parsePositive(value string) (int, error) {
	n, err := strconv.Atoi(value)
	if err != nil || n <= 0 {
		return 0, strconv.ErrSyntax
	}
	return n, nil
}
