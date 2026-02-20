from confluent_kafka.admin import AdminClient, AclBinding, AclOperation, AclPermissionType, ResourceType, ResourcePatternType
import os

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")

def setup_acls():
    admin = AdminClient({'bootstrap.servers': KAFKA_BROKER})
    
    # Define ACLs: Principal -> (Operation, Topic, Allow/Deny)
    acls = [
        # Traffic Service: Allow Read/Write on traffic topics
        AclBinding(ResourceType.TOPIC, "smartcity.traffic", ResourcePatternType.PREFIXED, "User:traffic-service", "*", AclOperation.READ, AclPermissionType.ALLOW),
        AclBinding(ResourceType.TOPIC, "smartcity.traffic", ResourcePatternType.PREFIXED, "User:traffic-service", "*", AclOperation.WRITE, AclPermissionType.ALLOW),
        
        # Deny Traffic Service access to Environmental
        AclBinding(ResourceType.TOPIC, "smartcity.environmental", ResourcePatternType.PREFIXED, "User:traffic-service", "*", AclOperation.ALL, AclPermissionType.DENY),
        
        # Urban Service
        AclBinding(ResourceType.TOPIC, "smartcity.urban_planning", ResourcePatternType.PREFIXED, "User:urban-service", "*", AclOperation.READ, AclPermissionType.ALLOW),
        
        # Environmental Service
        AclBinding(ResourceType.TOPIC, "smartcity.environmental", ResourcePatternType.PREFIXED, "User:env-service", "*", AclOperation.READ, AclPermissionType.ALLOW),
    ]
    
    try:
        futures = admin.create_acls(acls)
        for a, f in futures.items():
            try:
                f.result()
                print(f"Created ACL: {a}")
            except Exception as e:
                print(f"Failed to create ACL {a}: {e}")
    except Exception as e:
        print(f"ACL handling not fully supported by broker configuration or client: {e}")

if __name__ == "__main__":
    setup_acls()
