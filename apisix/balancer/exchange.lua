--
-- Licensed to the Apache Software Foundation (ASF) under one or more
-- contributor license agreements.  See the NOTICE file distributed with
-- this work for additional information regarding copyright ownership.
-- The ASF licenses this file to You under the Apache License, Version 2.0
-- (the "License"); you may not use this file except in compliance with
-- the License.  You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
--
-- 2021-08-20 刘俊林
-- exchange节点选择，如果已有绑定节点，则直接使用绑定节点，否则，使用roundrobin策略选择一个可用节点
--
local roundrobin = require("resty.roundrobin")
local core = require("apisix.core")
local etcd = require("apisix.core.etcd")
local nkeys = core.table.nkeys
local pairs = pairs

local _M = {}

function _M.new(up_nodes, upstream)
    core.log.warn("Balance Exchange Init.")

    local safe_limit = 0
    for _, weight in pairs(up_nodes) do
        -- the weight can be zero
        safe_limit = safe_limit + weight + 1
    end

    local picker = roundrobin:new(up_nodes)
    local nodes_count = nkeys(up_nodes)
    local bindPathPrefix = "/iops/exchange/bind";
    return {
        upstream = upstream,
        get = function(ctx)
            if ctx.balancer_tried_servers and ctx.balancer_tried_servers_count == nodes_count then
                return nil, "all upstream servers tried"
            end
            local bindPath;
            local user_id = ctx.var.arg_user_id;
            if user_id then
                bindPath = bindPathPrefix .. "/user/" .. user_id;
            else
                local app_id = ctx.var.arg_app_id;
                if not app_id or app_id == "" then
                    return nil, "app_id not found.";
                end ;
                local openid = ctx.var.arg_openid;
                if openid and openid ~= "" then
                    bindPath = bindPathPrefix .. "/wechat/" .. app_id .. "." .. openid;
                end ;
            end
            local server;
            local err;
            local res;
            local bindedValue;
            if bindPath then
                res, err = etcd.get(bindPath, false);
                if res and res.status == 200 and res.body.node.value then
                    bindedValue = res.body.node.value;
                    server = bindedValue.host .. ':' .. bindedValue.port;
                    core.log.warn("Get Bind[", bindPath, "][", server, ']')
                    return server;
                end
            end ;
            for i = 1, safe_limit do
                server, err = picker:find()
                if not server then
                    return nil, err
                end
                if ctx.balancer_tried_servers then
                    if not ctx.balancer_tried_servers[server] then
                        break
                    end
                else
                    break
                end
            end
            if bindPath and server and not bindedValue then
                local idx, _ = string.find(server, ':')
                if idx then
                    local host = string.sub(server, 1, idx - 1);
                    local port = tonumber(string.sub(server, idx + 1));
                    local bindServer = {};
                    bindServer.host = host;
                    bindServer.port = port;
                    bindServer.appName = "apisix";
                    core.log.warn("Set Bind[", bindPath, "][", host, ':', port, ']')
                    etcd.set(bindPath, bindServer, 90);
                end
            end
            return server
        end,
        after_balance = function(ctx, before_retry)
            if not before_retry then
                if ctx.balancer_tried_servers then
                    core.tablepool.release("balancer_tried_servers", ctx.balancer_tried_servers)
                    ctx.balancer_tried_servers = nil
                end
                return nil
            end
            if not ctx.balancer_tried_servers then
                ctx.balancer_tried_servers = core.tablepool.fetch("balancer_tried_servers", 0, 2)
            end
            ctx.balancer_tried_servers[ctx.balancer_server] = true
            ctx.balancer_tried_servers_count = (ctx.balancer_tried_servers_count or 0) + 1
        end,
        before_retry_next_priority = function(ctx)
            if ctx.balancer_tried_servers then
                core.tablepool.release("balancer_tried_servers", ctx.balancer_tried_servers)
                ctx.balancer_tried_servers = nil
            end
            ctx.balancer_tried_servers_count = 0
        end,
    }
end

return _M
