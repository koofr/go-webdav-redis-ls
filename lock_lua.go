package webdavredisls

import "github.com/gomodule/redigo/redis"

func nameKeyMacro(nameVar string) string {
	return `(prefix .. "` + namePrefix + `" .. ` + nameVar + `)`
}

func tokenKeyMacro(tokenVar string) string {
	return `(prefix .. "` + tokenPrefix + `" .. ` + tokenVar + `)`
}

var GetParentPathFunc = `
local slash_byte = string.byte("/")

local get_parent_path = function(path)
	local last_slash_idx = 0
	for i = string.len(path), 1, -1 do
		if string.byte(path, i) == slash_byte then
			last_slash_idx = i
			break
		end
	end

	if last_slash_idx == 0 or last_slash_idx == 1 then
		return "/"
	end

	return string.sub(path, 1, last_slash_idx - 1)
end
`

var CreateTokenFunc = `
local create_token = function(prefix, now_sec, root, duration_sec, is_zero_depth, owner_xml)
	local token = tonumber(redis.call("INCR", prefix.."` + nextTokenKey + `"))

	local path = root

	local is_first = true

	while true do
		local name_key = ` + nameKeyMacro("path") + `

		local ref_count = tonumber(redis.call("HINCRBY", name_key, "` + refCountKey + `", 1))

		local name_set_args = {}

		if ref_count == 1 then
			-- name did not exist, create it
			table.insert(name_set_args, "` + nameKey + `")
			table.insert(name_set_args, path)
			table.insert(name_set_args, "` + rootKey + `")
			table.insert(name_set_args, path)
			table.insert(name_set_args, "` + heldKey + `")
			table.insert(name_set_args, "` + falseValue + `")
		end

		local expiry_sec = 0

		if is_first then
			if duration_sec >= 0 then
				expiry_sec = now_sec + duration_sec
			end

			local zero_depth_value = "` + trueValue + `"
			if not is_zero_depth then
				zero_depth_value = "` + falseValue + `"
			end

			table.insert(name_set_args, "` + tokenKey + `")
			table.insert(name_set_args, token)
			table.insert(name_set_args, "` + durationKey + `")
			table.insert(name_set_args, duration_sec)
			table.insert(name_set_args, "` + ownerXMLKey + `")
			table.insert(name_set_args, owner_xml)
			table.insert(name_set_args, "` + zeroDepthKey + `")
			table.insert(name_set_args, zero_depth_value)
			table.insert(name_set_args, "` + expiryKey + `")
			table.insert(name_set_args, expiry_sec)
		end

		if #name_set_args > 0 then
			redis.call("HMSET", name_key, unpack(name_set_args))
		end

		if is_first then
			local token_key = ` + tokenKeyMacro("token") + `

			redis.call("SET", token_key, path)

			if duration_sec >= 0 then
				local expiry_zset_key = prefix .. "` + expiryZSetKey + `"
				redis.call("ZADD", expiry_zset_key, expiry_sec, path)
			end
		end

		if path == "/" then
			break
		end
		path = get_parent_path(path)
		is_first = false
	end

	return tostring(token)
end
`

var CanCreateFunc = `
local can_create = function(prefix, name, is_zero_depth)
	local path = name

	local is_first = true

	while true do
		local name_key = ` + nameKeyMacro("path") + `
		local root = redis.call("HGET", name_key, "` + rootKey + `")
		if root ~= false then
			local token = redis.call("HGET", name_key, "` + tokenKey + `")
			local node_is_zero_depth = redis.call("HGET", name_key, "` + zeroDepthKey + `") == "` + trueValue + `"

			if is_first then
				if token ~= false then
					-- The target node is already locked
					return false
				end
				if not is_zero_depth then
					-- The requested lock depth is infinite, and the fact that node exists
					-- (root ~= false) means that a descendent of the target node is locked.
					return false
				end
			elseif token ~= false and not node_is_zero_depth then
				-- An ancestor of the target node is locked with infinite depth.
				return false
			end
		end

		if path == "/" then
			break
		end
		path = get_parent_path(path)
		is_first = false
	end

	return true
end
`

var RemoveFunc = `
local remove = function(prefix, name, root, token, duration_sec)
	local token_key = ` + tokenKeyMacro("token") + `
	redis.call("DEL", token_key)

	local name_key = ` + nameKeyMacro("name") + `
	redis.call("HDEL", name_key, "` + tokenKey + `")

	if duration_sec >= 0 then
		local expiry_zset_key = prefix .. "` + expiryZSetKey + `"
		redis.call("ZREM", expiry_zset_key, name)
	end

	local path = root

	while true do
		local path_name_key = ` + nameKeyMacro("path") + `
		local ref_count = tonumber(redis.call("HINCRBY", path_name_key, "` + refCountKey + `", -1))

		if ref_count == 0 then
			redis.call("DEL", path_name_key)
		end

		if path == "/" then
			break
		end
		path = get_parent_path(path)
	end
end
`

var CollectExpiredNodesFunc = `
local collect_expired_nodes = function(prefix, now_sec)
	local expiry_zset_key = prefix .. "` + expiryZSetKey + `"
	while true do
		local names = redis.call("ZRANGEBYSCORE", expiry_zset_key, "-inf", now_sec, "LIMIT", 0, 100)
		if next(names) == nil then
			break
		end

		for _, name in ipairs(names) do
			local name_key = ` + nameKeyMacro("name") + `
			local res = redis.call("HMGET", name_key, "` + rootKey + `", "` + tokenKey + `", "` + durationKey + `")
			local root = res[1]
			local token = res[2]
			local duration_sec = tonumber(res[3])
			remove(prefix, name, root, token, duration_sec)
		end
	end
end
`

var HoldFunc = `
local hold = function(prefix, name, duration_sec)
	local name_key = ` + nameKeyMacro("name") + `
	local held_str = redis.call("HGET", name_key, "` + heldKey + `")
	if held_str == "` + trueValue + `" then
		error("inconsistent held state")
	end

	redis.call("HSET", name_key, "` + heldKey + `", "` + trueValue + `")

	if duration_sec >= 0 then
		local expiry_zset_key = prefix .. "` + expiryZSetKey + `"
		redis.call("ZREM", expiry_zset_key, name)
	end
end
`

var UnholdFunc = `
local unhold = function(prefix, name, duration_sec, expiry_sec)
	local name_key = ` + nameKeyMacro("name") + `
	local held_str = redis.call("HGET", name_key, "` + heldKey + `")
	if held_str ~= "` + trueValue + `" then
		error("inconsistent held state")
	end

	redis.call("HSET", name_key, "` + heldKey + `", "` + falseValue + `")

	if duration_sec >= 0 then
		local expiry_zset_key = prefix .. "` + expiryZSetKey + `"
		redis.call("ZADD", expiry_zset_key, expiry_sec, name)
	end
end
`

var CreateFunc = `
local create = function(prefix, now_sec, root, duration_sec, is_zero_depth, owner_xml)
	collect_expired_nodes(prefix, now_sec)

	if not can_create(prefix, root, is_zero_depth) then
		return "` + errLocked + `"
	end

	local token = create_token(prefix, now_sec, root, duration_sec, is_zero_depth, owner_xml)

	return token
end
`

var RefreshFunc = `
local refresh = function(prefix, now_sec, token, new_duration_sec)
	collect_expired_nodes(prefix, now_sec)

	local token_key = ` + tokenKeyMacro("token") + `

	local name = redis.call("GET", token_key)
	if not name then
		return "` + errNoSuchLock + `"
	end

	local name_key = ` + nameKeyMacro("name") + `
	local res = redis.call("HMGET", name_key, "` + rootKey + `", "` + durationKey + `", "` + ownerXMLKey + `", "` + zeroDepthKey + `", "` + heldKey + `")
	local root = res[1]
	local old_duration_sec = tonumber(res[2])
	local owner_xml = res[3]
	local zero_depth = res[4]
	local held = res[5] == "` + trueValue + `"

	if held then
		return "` + errLocked + `"
	end

	local expiry_zset_key = prefix .. "` + expiryZSetKey + `"

	if old_duration_sec >= 0 then
		redis.call("ZREM", expiry_zset_key, name)
	end

	local new_expiry_sec = 0

	if new_duration_sec >= 0 then
		new_expiry_sec = now_sec + new_duration_sec

		redis.call("ZADD", expiry_zset_key, new_expiry_sec, name)
	end

	redis.call("HMSET", name_key, "` + durationKey + `", new_duration_sec, "` + expiryKey + `", new_expiry_sec)

	return {
		"` + rootKey + `", root,
		"` + durationKey + `", tostring(new_duration_sec),
		"` + ownerXMLKey + `", owner_xml,
		"` + zeroDepthKey + `", zero_depth,
	}
end
`

var UnlockFunc = `
local unlock = function(prefix, now_sec, token)
	collect_expired_nodes(prefix, now_sec)

	local token_key = ` + tokenKeyMacro("token") + `

	local name = redis.call("GET", token_key)
	if not name then
		return "` + errNoSuchLock + `"
	end

	local name_key = ` + nameKeyMacro("name") + `
	local res = redis.call("HMGET", name_key, "` + rootKey + `", "` + durationKey + `", "` + heldKey + `")
	local root = res[1]
	local duration_sec = tonumber(res[2])
	local held = res[3] == "` + trueValue + `"

	if held then
		return "` + errLocked + `"
	end

	remove(prefix, name, root, token, duration_sec)
end
`

// LookupFunc returns the node n that locks the named resource, provided that n
// matches at least one of the given conditions and that lock isn't held by
// another party. Otherwise, it returns nil.
//
// n may be a parent of the named resource, if n is an infinite depth lock.
var LookupFunc = `
local lookup_token = function(prefix, lookup_name, token)
	local token_key = ` + tokenKeyMacro("token") + `

	local name = redis.call("GET", token_key)
	if not name then
		return nil
	end

	local name_key = ` + nameKeyMacro("name") + `
	local res = redis.call("HMGET", name_key, "` + rootKey + `", "` + durationKey + `", "` + zeroDepthKey + `", "` + heldKey + `")
	local root = res[1]
	local duration_sec = tonumber(res[2])
	local is_zero_depth = res[3] == "` + trueValue + `"
	local held = res[4] == "` + trueValue + `"

	if held then
		return nil
	end

	if lookup_name == root then
		return {root, duration_sec}
	end

	if is_zero_depth then
		return nil
	end

	local root_slash = root .. "/"
	-- has_prefix(lookup_name, root+"/")
	if root == "/" or (#lookup_name >= #root_slash and string.sub(lookup_name, 1, #root_slash) == root_slash) then
		return {root, duration_sec}
	end

	return nil
end

local lookup = function(prefix, lookup_name, condition_tokens)
	for _, token in ipairs(condition_tokens) do
		local res = lookup_token(prefix, lookup_name, token)
		if res ~= nil then
			return res
		end
	end

	return nil
end
`

var ConfirmFunc = `
local confirm = function(prefix, now_sec, name0, name1, condition_tokens)
	collect_expired_nodes(prefix, now_sec)

	local n0 = nil
	local n1 = nil

	if name0 ~= nil then
		n0 = lookup(prefix, name0, condition_tokens)
		if n0 == nil then
			return "` + errConfirmationFailed + `"
		end
	end
	if name1 ~= nil then
		n1 = lookup(prefix, name1, condition_tokens)
		if n1 == nil then
			return "` + errConfirmationFailed + `"
		end
	end

	-- Don't hold the same node twice.
	if n0 ~= nil and n1 ~= nil and n0[1] == n1[1] then
		n1 = nil
	end

	local res = {"", ""}

	if n0 ~= nil then
		hold(prefix, n0[1], n0[2])
		res[1] = n0[1]
	end
	if n1 ~= nil then
		hold(prefix, n1[1], n1[2])
		res[2] = n1[1]
	end

	return res
end
`

var ReleaseFunc = `
local release = function(prefix, name0, name1)
	if name0 ~= nil then
		local name0_key = ` + nameKeyMacro("name0") + `
		local res0 = redis.call("HMGET", name0_key, "` + durationKey + `", "` + expiryKey + `")
		local duration_sec0 = tonumber(res0[1])
		local expiry_sec0 = tonumber(res0[2])

		unhold(prefix, name0, duration_sec0, expiry_sec0)
	end

	if name1 ~= nil then
		local name1_key = ` + nameKeyMacro("name1") + `
		local res1 = redis.call("HMGET", name1_key, "` + durationKey + `", "` + expiryKey + `")
		local duration_sec1 = tonumber(res1[1])
		local expiry_sec1 = tonumber(res1[2])

		unhold(prefix, name1, duration_sec1, expiry_sec1)
	end
end
`

var CreateScript = redis.NewScript(0,
	GetParentPathFunc+
		RemoveFunc+
		CollectExpiredNodesFunc+
		CanCreateFunc+
		CreateTokenFunc+
		CreateFunc+
		`return create(ARGV[1], tonumber(ARGV[2]), ARGV[3], tonumber(ARGV[4]), ARGV[5] == "1", ARGV[6])`,
)

var RefreshScript = redis.NewScript(0,
	GetParentPathFunc+
		RemoveFunc+
		CollectExpiredNodesFunc+
		RefreshFunc+
		`return refresh(ARGV[1], tonumber(ARGV[2]), ARGV[3], tonumber(ARGV[4]))`,
)

var UnlockScript = redis.NewScript(0,
	GetParentPathFunc+
		RemoveFunc+
		CollectExpiredNodesFunc+
		UnlockFunc+
		`return unlock(ARGV[1], tonumber(ARGV[2]), ARGV[3])`,
)

var ConfirmScript = redis.NewScript(0,
	GetParentPathFunc+
		RemoveFunc+
		CollectExpiredNodesFunc+
		HoldFunc+
		LookupFunc+
		ConfirmFunc+
		`
		local condition_tokens_count = tonumber(ARGV[5])
		local condition_tokens = {unpack(ARGV, 6, 6 + condition_tokens_count)}
		local name0 = ARGV[3]
		if name0 == "" then
			name0 = nil
		end
		local name1 = ARGV[4]
		if name1 == "" then
			name1 = nil
		end
		return confirm(ARGV[1], tonumber(ARGV[2]), name0, name1, condition_tokens)
		`,
)

var ReleaseScript = redis.NewScript(0,
	GetParentPathFunc+
		RemoveFunc+
		CollectExpiredNodesFunc+
		UnholdFunc+
		ReleaseFunc+
		`
		local name0 = ARGV[2]
		if name0 == "" then
			name0 = nil
		end
		local name1 = ARGV[3]
		if name1 == "" then
			name1 = nil
		end
		return release(ARGV[1], name0, name1)
		`,
)
