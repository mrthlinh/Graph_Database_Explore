from neo4j import GraphDatabase
import glob

class neo4jFromFile(object):
    def __init__(self, uri, user, password):
        self.__driver = GraphDatabase.driver(uri, auth=(user,password))
    
    def close(self):
        self.__driver.close()
       
    def createfromCSV(self, filePath, function,delimiter = ','):
        fileType = filePath.split(".")[-1].lower()
        if fileType !='csv':
            raise Exception("File is not csv format")
        else:    
            import csv
            with open(filePath, encoding="utf-8-sig") as csv_file:
                csv_reader = csv.reader(csv_file, delimiter=delimiter)
                line_count = 0
                column = []
                with self.__driver.session() as session:
                    for row in csv_reader:
                        if line_count == 0:
                            column = row
                            # print(column)
                        else:
                            property_dict = dict(zip(column, row))
                            createNodeResult = session.write_transaction(function,property_dict)

                        line_count += 1
                    print(f'Processed {line_count} lines.')

    def createNodefromCSV(self, filePath, delimiter=','):
        self.createfromCSV(filePath, self.createNode ,delimiter = delimiter)

    def createRelationfromCSV(self, filePath, delimiter=','):
        self.createfromCSV(filePath, self.createRelation, delimiter = delimiter)
         
    @staticmethod
    def createNode(tx, property):
        label = property.get("label")
        label = "_".join(label.split())
        nodeName = property.get("id")
        del property['label']
        query = "CREATE (cus_{}:{})".format(nodeName,label)
        for key,value in property.items():
            query += " SET cus_{}.{} = '{}'".format(nodeName, key, str(value))
        result = tx.run(query)

    @staticmethod
    def createRelation(tx,property):
        node_out_id = property.get("out")
        node_out_label = property.get("outLabel")
        node_in_id = property.get("in")
        node_in_label = property.get("inLabel")
        rel_label = property.get("relationshipProp")
        del property["out"], property["outLabel"], property["in"], property["inLabel"], property["relationshipProp"]
        query = "MATCH (node_out:{}), (node_in:{}) WHERE node_out.id=\"{}\" AND node_in.id=\"{}\" MERGE (node_out)-[rel:{}]->(node_in)".format(node_out_label, node_in_label, node_out_id, node_in_id, rel_label)
        for key,value in property.items():
            query += " SET rel.{} = \"{}\"".format(key, str(value))
        result = tx.run(query)      

if __name__ == "__main__":
    uri = "bolt://localhost:7687"
    username = "root"
    password = "root"
    connector = neo4jFromFile(uri=uri,user=username, password=password)

    node_dir = glob.glob("/mnt/SSD/DataEconomyTraining/AML/AML_Graph/Node/*.csv")
    relation_dir = glob.glob("/mnt/SSD/DataEconomyTraining/AML/AML_Graph/Rel/*.csv")
    for file in node_dir:
        print("Processing filename: ",file)
        connector.createNodefromCSV(file)
    
    for file in relation_dir:
        print("Processing filename: ",file)
        connector.createRelationfromCSV(file)
    
    connector.close()