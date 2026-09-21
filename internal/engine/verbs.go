package engine

import "strings"

// 人工 FORCE 指令的 decision 字段采用受控的"动词:参数"语法，多个动词以分号分隔，
// 例如："PROTECT_CALL:CALL-007;ADMIT_CALL:CALL-007"。
// 自动策略只能读取这些动词，绝不能产生或覆盖它们。

// parseDecisionVerbs 把 decision 字符串拆成 动词 -> 参数 的映射。
func parseDecisionVerbs(decision string) map[string]string {
	verbs := map[string]string{}
	for _, part := range strings.Split(decision, ";") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		verb, arg, found := strings.Cut(part, ":")
		verb = strings.TrimSpace(verb)
		if verb == "" {
			continue
		}
		if found {
			verbs[verb] = strings.TrimSpace(arg)
		} else {
			verbs[verb] = ""
		}
	}
	return verbs
}

// hasDecisionVerb 判断 decision 是否包含某动词。
func hasDecisionVerb(decision, verb string) bool {
	_, ok := parseDecisionVerbs(decision)[verb]
	return ok
}

// decisionArg 返回某动词的参数，不存在返回空串。
func decisionArg(decision, verb string) string {
	return parseDecisionVerbs(decision)[verb]
}
