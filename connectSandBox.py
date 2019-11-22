from neo4j import GraphDatabase, basic_auth
import json
import os
class getDatafromNeo4jSandbox(object):
    def __init__(self, url, user, password):
        self.__driver = GraphDatabase.driver(url, auth=basic_auth(user,password))
    
    def close(self):
        self.__driver.close()

    # def executeCypherQuery(self, cypher_query, function):
    #     # cypher_query = "MATCH (node) RETURN node limit 100"
    #     with self.__driver.session() as session:
    #         createNodeResult = session.write_transaction(function)
        
    #     return createNodeResult

    def getAllNode(self, filePath):
        totalNumNodequery = "MATCH (node) RETURN count(node) as totalNode"
        getAllNodequery = "MATCH (node) RETURN node"
        with self.__driver.session() as session:
            totalNode = session.run(totalNumNodequery).data()[0]["totalNode"]
            results = session.run(getAllNodequery)
        values = results.data()
        count = 0
        dump_dict = {"data":[]}

        self.createFolder(filePath)
        with open(filePath, "w") as file:
            for record in values:
                record_dict = {"id":record["node"].id ,"labels":list(record["node"].labels)}
                record_dict["property"] = {k:v for k,v in record["node"].items()}
                # record_dict.update({k:v for k,v in record["node"].items()})
                dump_dict["data"].append(record_dict)
                count += 1
            json.dump(dump_dict,file)

        print(f"Processed {count} records")
        print(f"Number of total nodes: {totalNode}")

    def getAllRelationship(self, filePath):
        totalNumRelQuery = "match ()-[rel]-() return count(rel) as totalRel"
        getRelationshipQuery = "match ()-[rel:RATED]-() return rel"
        with self.__driver.session() as session:
            totalRel = session.run(totalNumRelQuery).data()[0]["totalRel"]
            results = session.run(getRelationshipQuery)

        values = results.data()
        count = 0
        dump_dict = {"data":[]}  

        # startnode id, end node id, type, properties
        self.createFolder(filePath)
        with open(filePath, "w") as file:
            for record in values:
                record_dict = {"id":record["rel"].id,
                                "startNode":record["rel"].start_node.id,
                                "endNode":record["rel"].end_node.id,
                                "type":record["rel"].type}
                record_dict["property"] = ({k:v for k,v in record["rel"].items()})
                dump_dict["data"].append(record_dict)
                count += 1
            json.dump(dump_dict,file)

        print(f"Processed {count} records")
        print(f"Number of total rel: {totalRel}")

        # print(values[0]['rel'].items())
    
    @staticmethod
    def createFolder(filePath):
        folderName = os.path.dirname(filePath)
        if not os.path.exists(folderName):
            os.makedirs(folderName)
            print(f"Created folder {folderName}")


if __name__ == "__main__":
    # Change credentials
    url = "bolt://100.25.160.95:34753"
    user, password = ("neo4j", "file-locomotives-boot")
    
    # sanBoxname is optional
    sandBoxname = 'MovieRecommendation'

    connector = getDatafromNeo4jSandbox(url, user,password)
    connector.getAllNode(sandBoxname + "/Nodes.json")
    connector.getAllRelationship(sandBoxname + "/Relations.json")
    connector.close()
