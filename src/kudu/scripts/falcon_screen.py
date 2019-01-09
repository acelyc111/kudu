#!/usr/bin/env python
# -*- coding: utf-8 -*-

import requests
import json
import re
import sys

#
# RESTful API doc: http://wiki.n.miui.com/pages/viewpage.action?pageId=66037692
# falcon ctrl api: http://dev.falcon.srv/doc/
#

# account info
serviceAccount = ""
serviceSeedMd5 = ""

###############################################################################

# global variables
falconServiceUrl = "http://falcon.srv"
# falconServiceUrl = "http://dev.falcon.srv"
kuduScreenId = 25748
KUDU_CLUSTER_ID = 37613
KUDU_TABLES_ID = 37638
KUDU_TSERVER_ID = 37639
KUDU_SYS_ID = 37640
screenIdList = {
    KUDU_CLUSTER_ID: "[cluster]",
    KUDU_TABLES_ID: [
        "[metrics_ab]",
        "[metrics_c]",
        "[metrics_d]",
        "[metrics_f]",
        "[metrics_ghijk]",
        "[metrics_l]",
        "[metrics_mn]",
        "[metrics_o]",
        "[metrics_r]",
        "[metrics_s]",
        "[metrics_u]",
        "[metrics_tw]"],
    KUDU_TSERVER_ID: "[tserver]",
    KUDU_SYS_ID: "[server-sys]"}
# kuduScreenId = 351
sessionId = ""
metaPort = ""
replicaPort = ""
collectorPort = ""


# return:
def get_session_id():
    url = falconServiceUrl + "/v1.0/auth/info"
    headers = {
        "Accept": "text/plain"
    }

    r = requests.get(url, headers=headers)
    if r.status_code != 200:
        print(
            "ERROR: get_session_id failed, status_code = %s, result:\n%s" %
            (r.status_code, r.text))
        sys.exit(1)

    c = r.headers['Set-Cookie']
    m = re.search('falconSessionId=([^;]+);', c)
    if m:
        global sessionId
        sessionId = m.group(1)
        print("INFO: sessionId =", sessionId)
    else:
        print("ERROR: get_session_id failed, cookie not set")
        sys.exit(1)


# return:
def auth_by_misso():
    url = falconServiceUrl + "/v1.0/auth/callback/misso"
    headers = {
        "Cookie": "falconSessionId=" +
        sessionId,
        "Authorization": serviceAccount +
        ";" +
        serviceSeedMd5 +
        ";" +
        serviceSeedMd5}

    r = requests.get(url, headers=headers)
    if r.status_code != 200:
        print(
            "ERROR: auth_by_misso failed, status_code = %s, result:\n%s" %
            (r.status_code, r.text))
        sys.exit(1)


# return:
def check_auth_info():
    url = falconServiceUrl + "/v1.0/auth/info"
    headers = {
        "Cookie": "falconSessionId=" + sessionId
    }

    r = requests.get(url, headers=headers)
    if r.status_code != 200:
        print(
            "ERROR: check_auth_info failed, status_code = %s, result:\n%s" %
            (r.status_code, r.text))
        sys.exit(1)

    j = json.loads(r.text)
    if "user" not in j or j["user"] is None or "name" not in j["user"] or j["user"]["name"] != serviceAccount:
        print("ERROR: check_auth_info failed, bad json result:\n%s" % r.text)
        sys.exit(1)


def login():
    get_session_id()
    auth_by_misso()
    check_auth_info()
    print("INFO: login succeed")


# return:
def logout():
    url = falconServiceUrl + "/v1.0/auth/logout"
    headers = {
        "Cookie": "falconSessionId=" + sessionId
    }

    r = requests.get(url, headers=headers)
    if r.status_code != 200:
        print(
            "ERROR: logout failed, status_code = %s, result:\n%s" %
            (r.status_code, r.text))
        sys.exit(1)

    print("INFO: logout succeed")


# return: screenId
def create_screen(screenName, scrid):
    url = falconServiceUrl + "/v1.0/dashboard/screen"
    headers = {
        "Cookie": "falconSessionId=" + sessionId
    }
    req = {
        "pid": scrid,
        "name": screenName
    }

    r = requests.post(url, headers=headers, data=json.dumps(req))
    if r.status_code != 200:
        print(
            "ERROR: create_screen failed, screenName = %s, status_code = %s, result:\n%s" %
            (screenName, r.status_code, r.text))
        sys.exit(1)

    j = json.loads(r.text)
    if "id" not in j:
        print(
            "ERROR: create_screen failed, screenName = %s, bad json result\n%s" %
            (screenName, r.text))
        sys.exit(1)

    screenId = j["id"]
    print(
        "INFO: create_screen succeed, screenName = %s, screenId = %s" %
        (screenName, screenId))
    return screenId


def parse_lines(file_name):
    lines = []
    for line in open(file_name):
        line.strip()
        if len(line) > 0:
            if line in lines:
                print("ERROR: bad file: duplicate line '%s'" % line)
                sys.exit(1)
            lines.append(line)
    return lines


# return: screenConfigs
def prepare_screen_config(
        clusterName,
        templateName,
        screenTemplateFile,
        tableListFile,
        masterListFile,
        tserverListFile):
    # tableList
    tableList = parse_lines(tableListFile)
    if len(tableList) == 0:
        print("WARN: empty table list file, will not create table level falcon screen")

    # masterList
    masterList = parse_lines(masterListFile)
    if len(masterList) == 0:
        print("ERROR: bad master list file: should be non-empty list")
        sys.exit(1)

    # tserverList
    tserverList = parse_lines(tserverListFile)
    if len(tserverList) == 0:
        print("ERROR: bad tserver list file: should be non-empty list")
        sys.exit(1)

    # template json
    jsonData = json.loads(open(screenTemplateFile).read())
    templateJson = jsonData['counter_templates']
    screensJson = jsonData['details']
    if not isinstance(screensJson, list) or len(screensJson) == 0:
        print(
            "ERROR: bad screen template json: [details] should be provided as non-empty list")
        sys.exit(1)

    screenConfigs = {}
    for screenJson in screensJson:
        # screen name
        screen = screenJson["screen"]
        if not isinstance(screen, (str, unicode)) or len(screen) == 0:
            print(
                "ERROR: bad json: [details][screen]: should be provided as non-empty str")
            sys.exit(1)
        screen = screen.replace("${cluster.name}", clusterName)
        if screen in screenConfigs:
            print("ERROR: duplicate screen '%s'" % screen)
            sys.exit(1)

        # graphs in screen
        graphConfigs = []
        position = 1
        for graphJson in screenJson['graphs']:
            # title
            title = graphJson["title"]
            if not isinstance(title, (str, unicode)) or len(title) == 0:
                print(
                    "ERROR: bad json: [details][%s][graphs][%s]: [title] should be provided as non-empty str" %
                    (screen, title))
                sys.exit(1)
            if title in graphConfigs:
                print("ERROR: duplicate title '%s'" % title)
                sys.exit(1)

            # endpoints
            endpoints = graphJson["endpoints"]
            newEndpoints = []
            for endpoint in endpoints:
                if len(endpoint) != 0:
                    if endpoint.find("${cluster.name}") != -1:
                        newEndpoints.append(
                            endpoint.replace(
                                "${cluster.name}",
                                clusterName))
                    elif endpoint.find("${for.each.master}") != -1:
                        newEndpoints += masterList
                    elif endpoint.find("${for.each.tserver}") != -1:
                        newEndpoints += tserverList
                    elif endpoint.find("${for.each.table}") != -1:
                        newEndpoints += tableList
                    else:
                        newEndpoints.append(endpoint)
            newEndpoints = list(set(newEndpoints))
            if len(newEndpoints) == 0:
                print(
                    "WARN: bad json: [details][%s][graphs][%s]: [endpoints] should be provided as non-empty list" %
                    (screen, title))

            # counters
            newCounters = []
            counters = graphJson["counters"]
            if not isinstance(counters, dict) or len(counters) == 0:
                print(
                    "ERROR: bad json: [details][%s][graphs][%s]: [counters] should be provided as non-empty list/dict" %
                    (screen, title))
                sys.exit(1)
            for counter in templateJson[counters["template"] if counters.has_key("template") else templateName]:
                newCounters.append(
                    counter.replace(
                        "${cluster.name}",
                        clusterName). replace(
                        "${level}",
                        counters["level"]))
            if len(newCounters) == 0:
                print(
                    "ERROR: bad json: [details][%s][graphs][%s]: [counters] should be provided as non-empty list" %
                    (screen, title))
                sys.exit(1)

            # graphType
            graphType = graphJson["graph_type"]
            if not isinstance(graphType, (str, unicode)):
                print(
                    "ERROR: bad json: [details][%s][graphs][%s]: [graph_type] should be provided as non-empty list" %
                    (screen, title))
                sys.exit(1)
            if graphType != "h" and graphType != "k" and graphType != "a":
                print(
                    "ERROR: bad json: [details][%s][graphs][%s]: [graph_type] should be 'h' or 'k' or 'a'" %
                    (screen, title))
                sys.exit(1)

            # method
            method = graphJson["method"]
            if not isinstance(method, (str, unicode)):
                print(
                    "ERROR: bad json: [details][%s][graphs][%s]: [method] should be provided as str" %
                    (screen, title))
                sys.exit(1)
            if method != "" and method != "sum":
                print(
                    "ERROR: bad json: [details][%s][graphs][%s]: [method] should be '' or 'sum'" %
                    (screen, title))
                sys.exit(1)

            # timespan
            timespan = graphJson["timespan"]
            if not isinstance(timespan, int) or timespan <= 0:
                print(
                    "ERROR: bad json: [details][%s][graphs][%s]: [timespan] should be provided as positive int" %
                    (screen, title))
                sys.exit(1)

            graphConfig = {}
            graphConfig["counters"] = newCounters
            graphConfig["endpoints"] = newEndpoints
            graphConfig["falcon_tags"] = ""
            graphConfig["graph_type"] = graphType
            graphConfig["method"] = method
            graphConfig["position"] = position
            graphConfig["timespan"] = timespan
            graphConfig["title"] = title
            graphConfigs.append(graphConfig)

            position += 1
        screenConfigs[screen] = graphConfigs

    return screenConfigs


# return: graphId
def create_graph(graphConfig):
    url = falconServiceUrl + "/v1.0/dashboard/graph"
    headers = {
        "Cookie": "falconSessionId=" + sessionId
    }

    r = requests.post(url, headers=headers, data=json.dumps(graphConfig))
    if r.status_code != 200:
        print(
            "ERROR: create_graph failed, graphTitle = \"%s\", status_code = %s, result:\n%s" %
            (graphConfig["title"], r.status_code, r.text))
        sys.exit(1)

    j = json.loads(r.text)
    if "id" not in j:
        print(
            "ERROR: create_graph failed, graphTitle = \"%s\", bad json result\n%s" %
            (graphConfig["title"], r.text))
        sys.exit(1)

    graphId = j["id"]
    print("INFO: create_graph succeed, graphTitle = \"%s\", graphId = %s"
          % (graphConfig["title"], graphId))

    # udpate graph position immediately
    graphConfig["id"] = graphId
    update_graph(graphConfig, "position")

    return graphId


# return: screen[]
def get_kudu_screens(scrid):
    url = falconServiceUrl + "/v1.0/dashboard/screen/pid/" + str(scrid)
    headers = {
        "Cookie": "falconSessionId=" + sessionId
    }

    r = requests.get(url, headers=headers)
    if r.status_code != 200:
        print(
            "ERROR: get_kudu_screens failed, status_code = %s, result:\n%s" %
            (r.status_code, r.text))
        sys.exit(1)

    j = json.loads(r.text)

    print("INFO: get_kudu_screens succeed, screenCount = %s" % len(j))
    return j


# return: graph[]
def get_screen_graphs(screenName, screenId):
    url = falconServiceUrl + "/v1.0/dashboard/graph/screen/" + str(screenId)
    headers = {
        "Cookie": "falconSessionId=" + sessionId
    }

    r = requests.get(url, headers=headers)
    if r.status_code != 200:
        print(
            "ERROR: get_screen_graphs failed, screenName = %s, screenId = %s, status_code = %s, result:\n%s" %
            (screenName, screenId, r.status_code, r.text))
        sys.exit(1)

    j = json.loads(r.text)

    print(
        "INFO: get_screen_graphs succeed, screenName = %s, screenId = %s, graphCount = %s" %
        (screenName, screenId, len(j)))
    return j


# return:
def delete_graph(graphTitle, graphId):
    url = falconServiceUrl + "/v1.0/dashboard/graph/" + str(graphId)
    headers = {
        "Cookie": "falconSessionId=" + sessionId
    }

    r = requests.delete(url, headers=headers)
    if r.status_code != 200 or r.text.find("delete success!") == -1:
        print(
            "ERROR: delete_graph failed, graphTitle = \"%s\", graphId = %s, status_code = %s, result:\n%s" %
            (graphTitle, graphId, r.status_code, r.text))
        sys.exit(1)

    print(
        "INFO: delete_graph succeed, graphTitle = \"%s\", graphId = %s" %
        (graphTitle, graphId))


# return:
def update_graph(graphConfig, updateReason):
    url = falconServiceUrl + "/v1.0/dashboard/graph/" + str(graphConfig["id"])
    headers = {
        "Cookie": "falconSessionId=" + sessionId
    }

    r = requests.put(url, headers=headers, data=json.dumps(graphConfig))
    if r.status_code != 200:
        print(
            "ERROR: update_graph failed, graphTitle = \"%s\", graphId = %s, status_code = %s, result:\n%s" %
            (graphConfig["title"], graphConfig["id"], r.status_code, r.text))
        sys.exit(1)

    j = json.loads(r.text)
    if "id" not in j:
        print(
            "ERROR: update_graph failed, graphTitle = \"%s\", graphId = %s, bad json result\n%s" %
            (graphConfig["title"], graphConfig["id"], r.text))
        sys.exit(1)

    print(
        "INFO: update_graph succeed, graphTitle = \"%s\", graphId = %s, updateReason = \"%s changed\"" %
        (graphConfig["title"], graphConfig["id"], updateReason))


# return: bool, reason
def is_equal(graph1, graph2):
    if graph1["title"] != graph2["title"]:
        return False, "title"
    if graph1["graph_type"] != graph2["graph_type"]:
        return False, "graph_type"
    if graph1["method"] != graph2["method"]:
        return False, "method"
    if graph1["position"] != graph2["position"]:
        return False, "position"
    if graph1["timespan"] != graph2["timespan"]:
        return False, "timespan"
    endpoints1 = graph1["endpoints"]
    endpoints2 = graph2["endpoints"]
    if len(endpoints1) != len(endpoints2):
        return False, "endpoints"
    for endpoint in endpoints1:
        if endpoint not in endpoints2:
            return False, "endpoints"
    counters1 = graph1["counters"]
    counters2 = graph2["counters"]
    if len(counters1) != len(counters2):
        return False, "counters"
    for counter in counters1:
        if counter not in counters2:
            return False, "counters"
    return True, ""


def create_screen_and_graphs(screenName, scrid, graphConfigs):

    # create screen
    screenId = create_screen(screenName, scrid)
    for graphConfig in graphConfigs:
        graphConfig["screen_id"] = screenId
        create_graph(graphConfig)
    print("INFO: %s graphs created for %s" % (len(graphConfigs), screenName))


def update_screen_and_graphs(screenName, screenId, graphConfigs):
    oldGraphConfigs = get_screen_graphs(screenName, screenId)
    if oldGraphConfigs is None:
        print(
            "ERROR: screen '%s' not exit, please create it first" %
            clusterName)
        sys.exit(1)

    # list -> dict
    oldGraphConfigsDict = {}
    newGraphConfigsDict = {}
    for graph in oldGraphConfigs:
        oldGraphConfigsDict[graph["title"]] = graph
    for graph in graphConfigs:
        newGraphConfigsDict[graph["title"]] = graph

    deleteConfigList = []
    createConfigList = []
    updateConfigList = []
    for graph in oldGraphConfigs:
        if not graph["title"] in newGraphConfigsDict:
            deleteConfigList.append((graph["title"], graph["graph_id"]))
    for graph in graphConfigs:
        if not graph["title"] in oldGraphConfigsDict:
            graph["screen_id"] = screenId
            createConfigList.append(graph)
        else:
            oldGraph = oldGraphConfigsDict[graph["title"]]
            equal, reason = is_equal(graph, oldGraph)
            if not equal:
                graph["id"] = oldGraph["graph_id"]
                graph["screen_id"] = screenId
                updateConfigList.append((graph, reason))

    for graphTitle, graphId in deleteConfigList:
        delete_graph(graphTitle, graphId)
    for graph in createConfigList:
        create_graph(graph)
    for graph, reason in updateConfigList:
        update_graph(graph, reason)

    print("INFO: %d graphs deleted, %d graphs created, %d graphs updated" %
          (len(deleteConfigList), len(createConfigList), len(updateConfigList)))


if __name__ == '__main__':
    if serviceAccount == "" or serviceSeedMd5 == "":
        print(
            "ERROR: please set 'serviceAccount' and 'serviceSeedMd5' in %s" %
            sys.argv[0])
        sys.exit(1)

    if len(sys.argv) != 7:
        print(
            "USAGE: python %s <cluster_name> <template_name> <screen_template_file> <master_list_file> <tserver_list_file> <table_list_file>" %
            sys.argv[0])
        sys.exit(1)

    clusterName = sys.argv[1]
    templateName = sys.argv[2]
    screenTemplateFile = sys.argv[3]
    masterListFile = sys.argv[4]
    tserverListFile = sys.argv[5]
    tableListFile = sys.argv[6]

    login()

    for scrid, scrNames in screenIdList.items():
        oldKuduScreens = get_kudu_screens(scrid)
        oldScreenName2Id = {}
        screenConfigs = prepare_screen_config(
            clusterName,
            templateName,
            screenTemplateFile,
            tableListFile,
            masterListFile,
            tserverListFile)
        for oldScreen in oldKuduScreens:
            oldScreenName2Id[oldScreen['name']] = oldScreen['id']
        if scrid == KUDU_TABLES_ID:
            for scrName in scrNames:
                screenName = clusterName + " " + scrName
                graphConfigs = screenConfigs[screenName]
                if screenName not in oldScreenName2Id:
                    # create screen
                    create_screen_and_graphs(screenName, scrid, graphConfigs)
                else:
                    # update screen
                    screenId = oldScreenName2Id[screenName]
                    update_screen_and_graphs(
                        screenName, screenId, graphConfigs)
        else:
            screenName = clusterName + " " + scrNames
            graphConfigs = screenConfigs[screenName]
            if screenName not in oldScreenName2Id:
                # create screen
                create_screen_and_graphs(screenName, scrid, graphConfigs)
            else:
                # update screen
                screenId = oldScreenName2Id[screenName]
                update_screen_and_graphs(screenName, screenId, graphConfigs)

    logout()
