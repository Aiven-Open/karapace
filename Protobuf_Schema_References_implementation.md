# Protobuf Schema References Implementation in Karapace



The aim of the project is to add common references functionality and specific Protobuf references implementation to Aiven’s Karapace.

The implementation plan consists of the following PRs to Aiven Karapace repository:

##### PR #1

 Add references support to all the required endpoints. The functionality is **about 90%** **common to all the protocols** (i.e. Avro, JSON, Protobuf, etc.). All the requests, responses, and ways of storing data in Kafka should be 1-to-1 compatible with Schema Registry:

- GET: return references data
- POST: Store references data to Kafka in 1-to-1
- Implement new endpoints specific to references. (REFERENCED BY: return list of schema IDs which reference a schema) 
- DELETE: don’t allow to delete schemas that are referenced by others, etc.
- Tests

##### PR #2

Support references (imports) in **Protobuf** schemas. 

- Resolve imported entities using references
- POST: Validate Protobuf schema with references
- POST: Compatibility check with references
- Tests

##### PR #3 

Support Known Dependencies (KD) in **Protobuf** schemas

- Research an approach to storing and resolving KD
- Resolve imported entities using KD
- Tests

##### PR #4 

Serialize and deserialize using **Protobuf** schemas with imports

- Resolve references by protoc to build Python modules
- Resolve KD by protoc to build Python modules
- Tests