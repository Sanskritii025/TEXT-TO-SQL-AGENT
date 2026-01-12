package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"log"
	"os"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

// newDriver creates a connection to Neo4j Aura
func newDriver() (neo4j.DriverWithContext, context.Context, func()) {
	//uri := os.Getenv("NEO4J_URI")
	//user := os.Getenv("NEO4J_USER")
	//password := os.Getenv("NEO4J_PASSWORD")

	uri := "neo4j+s://efba5844.databases.neo4j.io"
	user := "neo4j"
	password := "zB8B-ym5HfSVRGQiKUGQNTZCvG32DXu3AdB6hYacMuE"

	driver, err := neo4j.NewDriverWithContext(uri, neo4j.BasicAuth(user, password, ""))
	if err != nil {
		log.Fatalf("Failed to create driver: %v", err)
	}
	ctx := context.Background()
	return driver, ctx, func() { driver.Close(ctx) }
}

// readCSV loads the CSV file into a slice of string arrays
func readCSV(filename string) ([][]string, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	reader := csv.NewReader(f)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}
	return records, nil
}

// createRelationship creates a single relationship between two nodes
func createRelationship(
	driver neo4j.DriverWithContext,
	ctx context.Context,
	fromLabel, fromKey, toLabel, toKey, relType string,
	fromVal, toVal string,
) error {
	session := driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(ctx)

	_, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		query := fmt.Sprintf(`
			MATCH (a:%s {%s: $fromVal})
			MATCH (b:%s {%s: $toVal})
			MERGE (a)-[:%s]->(b)`,
			fromLabel, fromKey, toLabel, toKey, relType)
		_, err := tx.Run(ctx, query, map[string]any{
			"fromVal": fromVal,
			"toVal":   toVal,
		})
		return nil, err
	})
	return err
}

// Customer -> Account
func importCustomerHasAccount(driver neo4j.DriverWithContext, ctx context.Context, path string) {
	records, err := readCSV(path)
	if err != nil {
		log.Fatalf("Error reading %s: %v", path, err)
	}
	for i, row := range records {
		if i == 0 {
			continue
		}
		acctID := row[0]
		custID := row[2]
		if acctID == "" || custID == "" {
			continue
		}
		if err := createRelationship(driver, ctx,
			"Customer", "CustomerID",
			"Account", "AccountID",
			"HAS_ACCOUNT", custID, acctID); err != nil {
			log.Println("HAS_ACCOUNT failed:", err)
		}
	}
	fmt.Println("✅ HAS_ACCOUNT relationships created")
}

// Account -> SalesRep
func importAccountOwnedBySalesRep(driver neo4j.DriverWithContext, ctx context.Context, path string) {
	records, _ := readCSV(path)
	for i, row := range records {
		if i == 0 {
			continue
		}
		acctRegion := row[3]
		territoryID := row[3]
		if acctRegion == "" || territoryID == "" {
			continue
		}
		if err := createRelationship(driver, ctx,
			"Account", "Region",
			"SalesRep", "TerritoryID",
			"OWNED_BY", acctRegion, territoryID); err != nil {
			log.Println("OWNED_BY failed:", err)
		}
	}
	fmt.Println("✅ OWNED_BY relationships created")
}

// Opportunity -> SalesOrder
func importOpportunityConvertedTo(driver neo4j.DriverWithContext, ctx context.Context, path string) {
	records, _ := readCSV(path)
	for i, row := range records {
		if i == 0 {
			continue
		}
		soID := row[0]
		oppID := row[1]
		if oppID == "" || soID == "" {
			continue
		}
		if err := createRelationship(driver, ctx,
			"Opportunity", "OpportunityID",
			"SalesOrder", "SalesOrderID",
			"CONVERTED_TO", oppID, soID); err != nil {
			log.Println("CONVERTED_TO failed:", err)
		}
	}
	fmt.Println("✅ CONVERTED_TO relationships created")
}

// SalesOrder -> Product
func importSalesOrderContains(driver neo4j.DriverWithContext, ctx context.Context, path string) {
	records, _ := readCSV(path)
	for i, row := range records {
		if i == 0 {
			continue
		}
		soID := row[0]
		prodID := row[1]
		if soID == "" || prodID == "" {
			continue
		}
		if err := createRelationship(driver, ctx,
			"SalesOrder", "SalesOrderID",
			"Product", "ProductID",
			"CONTAINS", soID, prodID); err != nil {
			log.Println("CONTAINS failed:", err)
		}
	}
	fmt.Println("✅ CONTAINS relationships created")
}

// Invoice -> SalesOrder
func importInvoiceGeneratedFrom(driver neo4j.DriverWithContext, ctx context.Context, path string) {
	records, _ := readCSV(path)
	for i, row := range records {
		if i == 0 {
			continue
		}
		invID := row[0]
		soID := row[1]
		if invID == "" || soID == "" {
			continue
		}
		if err := createRelationship(driver, ctx,
			"Invoice", "InvoiceID",
			"SalesOrder", "SalesOrderID",
			"GENERATED_FROM", invID, soID); err != nil {
			log.Println("GENERATED_FROM failed:", err)
		}
	}
	fmt.Println("✅ GENERATED_FROM relationships created")
}

// SalesOrder -> Customer
func importSalesOrderBilledTo(driver neo4j.DriverWithContext, ctx context.Context, path string) {
	records, _ := readCSV(path)
	for i, row := range records {
		if i == 0 {
			continue
		}
		soID := row[0]
		custID := row[2]
		if soID == "" || custID == "" {
			continue
		}
		if err := createRelationship(driver, ctx,
			"SalesOrder", "SalesOrderID",
			"Customer", "CustomerID",
			"BILLED_TO", soID, custID); err != nil {
			log.Println("BILLED_TO failed:", err)
		}
	}
	fmt.Println("✅ BILLED_TO relationships created")
}

func main() {
	driver, ctx, closeFn := newDriver()
	defer closeFn()

	importCustomerHasAccount(driver, ctx, "accounts.csv")
	//importAccountOwnedBySalesRep(driver, ctx, "accounts.csv")
	importOpportunityConvertedTo(driver, ctx, "sales_orders.csv")
	//importSalesOrderContains(driver, ctx, "products.csv")
	importInvoiceGeneratedFrom(driver, ctx, "invoices.csv")
	//importSalesOrderBilledTo(driver, ctx, "sales_orders.csv")

	fmt.Println("🚀 All relationships created successfully in Aura DB")
}
