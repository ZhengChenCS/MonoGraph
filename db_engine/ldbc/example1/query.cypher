MATCH (p:Place {id: $placeId})<-[:personIsLocatedIn]-(person:Person)-[:likeComment]->(c:Comment)
RETURN c.content