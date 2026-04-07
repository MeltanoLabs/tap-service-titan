<!--These prompts can be used to generate streams using AI.
    They've been tested with a fair degree of success using Claude 3.5 Sonnet.
    Streams generated in this way should still be tested and checked for accuracy.
-->

I'm going to give you a part of a URL path and then a json schema for the data in the response model. I want you to give me code for a stream built using the Meltano SDK to match. I'll provide a few examples first.

# Example 1

URL path segment: /tenant/{tenant}/export/invoices
JSON schema:

```{'required': ['id', 'createdOn', 'sentStatus', 'reviewStatus', 'active'], 'type': 'object', 'properties': {'id': {'type': 'integer', 'format': 'int64'}, 'syncStatus': {'type': 'string', 'nullable': True}, 'summary': {'type': 'string', 'format': 'html', 'nullable': True}, 'referenceNumber': {'type': 'string', 'nullable': True}, 'invoiceDate': {'type': 'string', 'format': 'date-time', 'nullable': True}, 'dueDate': {'type': 'string', 'format': 'date-time', 'nullable': True}, 'subTotal': {'type': 'string', 'nullable': True}, 'salesTax': {'type': 'string', 'nullable': True}, 'salesTaxCode': {'oneOf': [{'required': ['id', 'taxRate'], 'type': 'object', 'properties': {'id': {'type': 'integer', 'format': 'int64'}, 'name': {'type': 'string', 'nullable': True}, 'taxRate': {'type': 'number', 'format': 'decimal'}}, 'additionalProperties': False}], 'nullable': True}, 'total': {'type': 'string', 'nullable': True}, 'balance': {'type': 'string', 'nullable': True}, 'invoiceType': {'oneOf': [{'required': ['id'], 'type': 'object', 'properties': {'id': {'type': 'integer', 'format': 'int64'}, 'name': {'type': 'string', 'nullable': True}}, 'additionalProperties': False}], 'nullable': True}, 'customer': {'oneOf': [{'required': ['id'], 'type': 'object', 'properties': {'id': {'type': 'integer', 'format': 'int64'}, 'name': {'type': 'string', 'nullable': True}}, 'additionalProperties': False}], 'nullable': True}, 'customerAddress': {'oneOf': [{'type': 'object', 'properties': {'street': {'type': 'string', 'nullable': True}, 'unit': {'type': 'string', 'nullable': True}, 'city': {'type': 'string', 'nullable': True}, 'state': {'type': 'string', 'nullable': True}, 'zip': {'type': 'string', 'nullable': True}, 'country': {'type': 'string', 'nullable': True}}, 'additionalProperties': False}], 'nullable': True}, 'location': {'oneOf': [{'required': ['id'], 'type': 'object', 'properties': {'id': {'type': 'integer', 'format': 'int64'}, 'name': {'type': 'string', 'nullable': True}}, 'additionalProperties': False}], 'nullable': True}, 'locationAddress': {'oneOf': [{'type': 'object', 'properties': {'street': {'type': 'string', 'nullable': True}, 'unit': {'type': 'string', 'nullable': True}, 'city': {'type': 'string', 'nullable': True}, 'state': {'type': 'string', 'nullable': True}, 'zip': {'type': 'string', 'nullable': True}, 'country': {'type': 'string', 'nullable': True}}, 'additionalProperties': False}], 'nullable': True}, 'businessUnit': {'oneOf': [{'required': ['id'], 'type': 'object', 'properties': {'id': {'type': 'integer', 'format': 'int64'}, 'name': {'type': 'string', 'nullable': True}}, 'additionalProperties': False}], 'nullable': True}, 'termName': {'type': 'string', 'nullable': True}, 'createdBy': {'type': 'string', 'nullable': True}, 'batch': {'oneOf': [{'required': ['id'], 'type': 'object', 'properties': {'id': {'type': 'integer', 'format': 'int64'}, 'number': {'type': 'string', 'nullable': True}, 'name': {'type': 'string', 'nullable': True}}, 'additionalProperties': False}], 'nullable': True}, 'depositedOn': {'type': 'string', 'format': 'date-time', 'nullable': True}, 'createdOn': {'type': 'string', 'format': 'date-time'}, 'modifiedOn': {'type': 'string', 'format': 'date-time', 'nullable': True}, 'adjustmentToId': {'type': 'integer', 'format': 'int64', 'nullable': True}, 'job': {'oneOf': [{'required': ['id'], 'type': 'object', 'properties': {'id': {'type': 'integer', 'format': 'int64'}, 'number': {'type': 'string', 'nullable': True}, 'type': {'type': 'string', 'nullable': True}}, 'additionalProperties': False}], 'nullable': True}, 'projectId': {'type': 'integer', 'format': 'int64', 'nullable': True}, 'royalty': {'oneOf': [{'type': 'object', 'properties': {'status': {'type': 'string', 'nullable': True}, 'date': {'type': 'string', 'format': 'date-time', 'nullable': True}, 'sentOn': {'type': 'string', 'format': 'date-time', 'nullable': True}, 'memo': {'type': 'string', 'nullable': True}}, 'additionalProperties': False}], 'nullable': True}, 'employeeInfo': {'oneOf': [{'required': ['id', 'modifiedOn'], 'type': 'object', 'properties': {'id': {'type': 'integer', 'format': 'int64'}, 'name': {'type': 'string', 'nullable': True}, 'modifiedOn': {'type': 'string', 'format': 'date-time'}}, 'additionalProperties': False}], 'nullable': True}, 'commissionEligibilityDate': {'type': 'string', 'nullable': True}, 'sentStatus': {'enum': ['NotSent', 'Sent', 'Opened'], 'type': 'string', 'description': '', 'x-enumNames': ['NotSent', 'Sent', 'Opened']}, 'reviewStatus': {'enum': ['NeedsReview', 'OnHold', 'Reviewed'], 'type': 'string', 'description': '', 'x-enumNames': ['NeedsReview', 'OnHold', 'Reviewed']}, 'assignedTo': {'oneOf': [{'required': ['id'], 'type': 'object', 'properties': {'id': {'type': 'integer', 'format': 'int64'}, 'name': {'type': 'string', 'nullable': True}}, 'additionalProperties': False}], 'nullable': True}, 'items': {'type': 'array', 'items': {'required': ['id', 'type', 'skuId', 'inventory', 'taxable', 'membershipTypeId', 'modifiedOn', 'order'], 'type': 'object', 'properties': {'id': {'type': 'integer', 'format': 'int64'}, 'description': {'type': 'string', 'format': 'html', 'nullable': True}, 'quantity': {'type': 'string', 'nullable': True}, 'cost': {'type': 'string', 'nullable': True}, 'totalCost': {'type': 'string', 'nullable': True}, 'inventoryLocation': {'type': 'string', 'nullable': True}, 'price': {'type': 'string', 'nullable': True}, 'type': {'enum': ['Service', 'Material', 'Equipment', 'PriceModifier', 'Unspecified'], 'type': 'string', 'description': "Indicates a type's item", 'x-enumNames': ['Service', 'Material', 'Equipment', 'PriceModifier', 'Unspecified']}, 'skuName': {'type': 'string', 'nullable': True}, 'skuId': {'type': 'integer', 'format': 'int64'}, 'total': {'type': 'string', 'nullable': True}, 'inventory': {'type': 'boolean'}, 'taxable': {'type': 'boolean'}, 'generalLedgerAccount': {'oneOf': [{'required': ['id'], 'type': 'object', 'properties': {'id': {'type': 'integer', 'format': 'int64'}, 'name': {'type': 'string', 'nullable': True}, 'number': {'type': 'string', 'nullable': True}, 'type': {'type': 'string', 'nullable': True}, 'detailType': {'type': 'string', 'nullable': True}}, 'additionalProperties': False}], 'nullable': True}, 'costOfSaleAccount': {'oneOf': [{'required': ['id'], 'type': 'object', 'properties': {'id': {'type': 'integer', 'format': 'int64'}, 'name': {'type': 'string', 'nullable': True}, 'number': {'type': 'string', 'nullable': True}, 'type': {'type': 'string', 'nullable': True}, 'detailType': {'type': 'string', 'nullable': True}}, 'additionalProperties': False}], 'nullable': True}, 'assetAccount': {'oneOf': [{'required': ['id'], 'type': 'object', 'properties': {'id': {'type': 'integer', 'format': 'int64'}, 'name': {'type': 'string', 'nullable': True}, 'number': {'type': 'string', 'nullable': True}, 'type': {'type': 'string', 'nullable': True}, 'detailType': {'type': 'string', 'nullable': True}}, 'additionalProperties': False}], 'nullable': True}, 'membershipTypeId': {'type': 'integer', 'format': 'int64'}, 'itemGroup': {'oneOf': [{'required': ['rootId'], 'type': 'object', 'properties': {'rootId': {'type': 'integer', 'format': 'int64'}, 'name': {'type': 'string', 'nullable': True}}, 'additionalProperties': False}], 'nullable': True}, 'displayName': {'type': 'string', 'nullable': True}, 'soldHours': {'type': 'number', 'format': 'decimal', 'nullable': True}, 'modifiedOn': {'type': 'string', 'format': 'date-time'}, 'serviceDate': {'type': 'string', 'format': 'date-time', 'nullable': True}, 'order': {'type': 'integer', 'format': 'int32'}, 'businessUnit': {'oneOf': [{'required': ['id'], 'type': 'object', 'properties': {'id': {'type': 'integer', 'format': 'int64'}, 'name': {'type': 'string', 'nullable': True}}, 'additionalProperties': False}], 'nullable': True}}, 'additionalProperties': False}, 'nullable': True}, 'customFields': {'type': 'array', 'items': {'type': 'object', 'properties': {'name': {'type': 'string', 'nullable': True}, 'value': {'type': 'string', 'nullable': True}}, 'additionalProperties': False}, 'nullable': True}, 'active': {'type': 'boolean', 'description': 'Whether this invoice is active, or not anymore.'}}, 'additionalProperties': False}
```

Resulting stream code:

```
class InvoicesStream(ServiceTitanExportStream):
    """Define invoices stream."""

    name = "invoices"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"

    schema = th.PropertiesList(
        th.Property("id", th.IntegerType),
        th.Property("syncStatus", th.StringType),
        th.Property("summary", th.StringType),
        th.Property("referenceNumber", th.StringType),
        th.Property("invoiceDate", th.StringType),
        th.Property("dueDate", th.StringType),
        th.Property("subTotal", th.StringType),
        th.Property("salesTax", th.StringType),
        th.Property(
            "salesTaxCode",
            th.ObjectType(
                th.Property("id", th.IntegerType),
                th.Property("name", th.StringType),
                th.Property("taxRate", th.NumberType),
            ),
        ),
        th.Property("total", th.StringType),
        th.Property("balance", th.StringType),
        th.Property(
            "invoiceType",
            th.ObjectType(
                th.Property("id", th.IntegerType),
                th.Property("name", th.StringType),
            ),
        ),
        th.Property(
            "customer",
            th.ObjectType(
                th.Property("id", th.IntegerType),
                th.Property("name", th.StringType),
            ),
        ),
        th.Property(
            "customerAddress",
            th.ObjectType(
                th.Property("street", th.StringType),
                th.Property("unit", th.StringType),
                th.Property("city", th.StringType),
                th.Property("state", th.StringType),
                th.Property("zip", th.StringType),
                th.Property("country", th.StringType),
            ),
        ),
        th.Property(
            "location",
            th.ObjectType(
                th.Property("id", th.IntegerType),
                th.Property("name", th.StringType),
            ),
        ),
        th.Property(
            "locationAddress",
            th.ObjectType(
                th.Property("street", th.StringType),
                th.Property("unit", th.StringType),
                th.Property("city", th.StringType),
                th.Property("state", th.StringType),
                th.Property("zip", th.StringType),
                th.Property("country", th.StringType),
            ),
        ),
        th.Property(
            "businessUnit",
            th.ObjectType(
                th.Property("id", th.IntegerType),
                th.Property("name", th.StringType),
            ),
        ),
        th.Property(
            "batch",
            th.ObjectType(
                th.Property("id", th.IntegerType),
                th.Property("number", th.StringType),
                th.Property("name", th.StringType),
            ),
        ),
        th.Property("depositedOn", th.StringType),
        th.Property("createdOn", th.StringType),
        th.Property("modifiedOn", th.DateTimeType),
        th.Property("adjustmentToId", th.IntegerType),
        th.Property(
            "job",
            th.ObjectType(
                th.Property("id", th.IntegerType),
                th.Property("number", th.StringType),
                th.Property("type", th.StringType),
            ),
        ),
        th.Property("projectId", th.IntegerType),
        th.Property(
            "royalty",
            th.ObjectType(
                th.Property("status", th.StringType),
                th.Property("date", th.StringType),
                th.Property("sentOn", th.StringType),
                th.Property("memo", th.StringType),
            ),
        ),
        th.Property(
            "employeeInfo",
            th.ObjectType(
                th.Property("id", th.IntegerType),
                th.Property("name", th.StringType),
                th.Property("modifiedOn", th.DateTimeType),
            ),
        ),
        th.Property("commissionEligibilityDate", th.StringType),
        th.Property("sentStatus", th.StringType),  # Assuming this is a string type
        th.Property("reviewStatus", th.StringType),  # Assuming this is a string type
        th.Property(
            "assignedTo",
            th.ObjectType(
                th.Property("id", th.IntegerType),
                th.Property("name", th.StringType),
            ),
        ),
        th.Property(
            "items",
            th.ArrayType(
                th.ObjectType(
                    th.Property("id", th.IntegerType),
                    th.Property("description", th.StringType),
                    th.Property("quantity", th.StringType),
                    th.Property("cost", th.StringType),
                    th.Property("totalCost", th.StringType),
                    th.Property("inventoryLocation", th.StringType),
                    th.Property("price", th.StringType),
                    th.Property(
                        "type", th.StringType
                    ),  # Assuming this is a string type
                    th.Property("skuName", th.StringType),
                    th.Property("skuId", th.IntegerType),
                    th.Property("total", th.StringType),
                    th.Property("inventory", th.BooleanType),
                    th.Property("taxable", th.BooleanType),
                    th.Property(
                        "generalLedgerAccount",
                        th.ObjectType(
                            th.Property("id", th.IntegerType),
                            th.Property("name", th.StringType),
                            th.Property("number", th.StringType),
                            th.Property("type", th.StringType),
                            th.Property("detailType", th.StringType),
                        ),
                    ),
                    th.Property(
                        "costOfSaleAccount",
                        th.ObjectType(
                            th.Property("id", th.IntegerType),
                            th.Property("name", th.StringType),
                            th.Property("number", th.StringType),
                            th.Property("type", th.StringType),
                            th.Property("detailType", th.StringType),
                        ),
                    ),
                    th.Property(
                        "assetAccount",
                        th.ObjectType(
                            th.Property("id", th.IntegerType),
                            th.Property("name", th.StringType),
                            th.Property("number", th.StringType),
                            th.Property("type", th.StringType),
                            th.Property("detailType", th.StringType),
                        ),
                    ),
                    th.Property("membershipTypeId", th.IntegerType),
                    th.Property(
                        "itemGroup",
                        th.ObjectType(
                            th.Property("rootId", th.IntegerType),
                            th.Property("name", th.StringType),
                        ),
                    ),
                    th.Property("displayName", th.StringType),
                    th.Property("soldHours", th.NumberType),
                    th.Property("modifiedOn", th.DateTimeType),
                    th.Property("serviceDate", th.StringType),
                    th.Property("order", th.IntegerType),
                    th.Property(
                        "businessUnit",
                        th.ObjectType(
                            th.Property("id", th.IntegerType),
                            th.Property("name", th.StringType),
                        ),
                    ),
                )
            ),
        ),
        th.Property(
            "customFields",
            th.ArrayType(
                th.ObjectType(
                    th.Property("name", th.StringType),
                    th.Property("value", th.StringType),
                )
            ),
        ),
        th.Property("active", th.BooleanType),
    ).to_dict()

    @cached_property
    def path(self) -> str:
        """Return the API path for the stream."""
        return f"/accounting/v2/tenant/{self._tap.config['tenant_id']}/export/invoices"
```

# Example 2

URL path segment: /tenant/{tenant}/export/invoice-items
JSON schema:

```
{'required': ['id', 'type', 'skuId', 'inventory', 'taxable', 'membershipTypeId', 'modifiedOn', 'order', 'active', 'invoiceId'], 'type': 'object', 'properties': {'id': {'type': 'integer', 'format': 'int64'}, 'description': {'type': 'string', 'format': 'html', 'nullable': True}, 'quantity': {'type': 'string', 'nullable': True}, 'cost': {'type': 'string', 'nullable': True}, 'totalCost': {'type': 'string', 'nullable': True}, 'inventoryLocation': {'type': 'string', 'nullable': True}, 'price': {'type': 'string', 'nullable': True}, 'type': {'enum': ['Service', 'Material', 'Equipment', 'PriceModifier', 'Unspecified'], 'type': 'string', 'description': "Indicates a type's item", 'x-enumNames': ['Service', 'Material', 'Equipment', 'PriceModifier', 'Unspecified']}, 'skuName': {'type': 'string', 'nullable': True}, 'skuId': {'type': 'integer', 'format': 'int64'}, 'total': {'type': 'string', 'nullable': True}, 'inventory': {'type': 'boolean'}, 'taxable': {'type': 'boolean'}, 'generalLedgerAccount': {'oneOf': [{'required': ['id'], 'type': 'object', 'properties': {'id': {'type': 'integer', 'format': 'int64'}, 'name': {'type': 'string', 'nullable': True}, 'number': {'type': 'string', 'nullable': True}, 'type': {'type': 'string', 'nullable': True}, 'detailType': {'type': 'string', 'nullable': True}}, 'additionalProperties': False}], 'nullable': True}, 'costOfSaleAccount': {'oneOf': [{'required': ['id'], 'type': 'object', 'properties': {'id': {'type': 'integer', 'format': 'int64'}, 'name': {'type': 'string', 'nullable': True}, 'number': {'type': 'string', 'nullable': True}, 'type': {'type': 'string', 'nullable': True}, 'detailType': {'type': 'string', 'nullable': True}}, 'additionalProperties': False}], 'nullable': True}, 'assetAccount': {'oneOf': [{'required': ['id'], 'type': 'object', 'properties': {'id': {'type': 'integer', 'format': 'int64'}, 'name': {'type': 'string', 'nullable': True}, 'number': {'type': 'string', 'nullable': True}, 'type': {'type': 'string', 'nullable': True}, 'detailType': {'type': 'string', 'nullable': True}}, 'additionalProperties': False}], 'nullable': True}, 'membershipTypeId': {'type': 'integer', 'format': 'int64'}, 'itemGroup': {'oneOf': [{'required': ['rootId'], 'type': 'object', 'properties': {'rootId': {'type': 'integer', 'format': 'int64'}, 'name': {'type': 'string', 'nullable': True}}, 'additionalProperties': False}], 'nullable': True}, 'displayName': {'type': 'string', 'nullable': True}, 'soldHours': {'type': 'number', 'format': 'decimal', 'nullable': True}, 'modifiedOn': {'type': 'string', 'format': 'date-time'}, 'serviceDate': {'type': 'string', 'format': 'date-time', 'nullable': True}, 'order': {'type': 'integer', 'format': 'int32'}, 'businessUnit': {'oneOf': [{'required': ['id'], 'type': 'object', 'properties': {'id': {'type': 'integer', 'format': 'int64'}, 'name': {'type': 'string', 'nullable': True}}, 'additionalProperties': False}], 'nullable': True}, 'active': {'type': 'boolean'}, 'invoiceId': {'type': 'integer', 'format': 'int64'}}, 'additionalProperties': False}
```

Resulting code:

```
class InvoiceItemsStream(ServiceTitanExportStream):
    """Define invoice items stream."""

    name = "invoice_items"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"

    schema = th.PropertiesList(
        th.Property("id", th.IntegerType),
        th.Property("description", th.StringType),
        th.Property("quantity", th.StringType),
        th.Property("cost", th.StringType),
        th.Property("totalCost", th.StringType),
        th.Property("inventoryLocation", th.StringType),
        th.Property("price", th.StringType),
        th.Property("type", th.StringType),
        th.Property("skuName", th.StringType),
        th.Property("skuId", th.IntegerType),
        th.Property("total", th.StringType),
        th.Property("inventory", th.BooleanType),
        th.Property("taxable", th.BooleanType),
        th.Property(
            "generalLedgerAccount",
            th.ObjectType(
                th.Property("id", th.IntegerType),
                th.Property("name", th.StringType),
                th.Property("number", th.StringType),
                th.Property("type", th.StringType),
                th.Property("detailType", th.StringType),
            ),
        ),
        th.Property(
            "costOfSaleAccount",
            th.ObjectType(
                th.Property("id", th.IntegerType),
                th.Property("name", th.StringType),
                th.Property("number", th.StringType),
                th.Property("type", th.StringType),
                th.Property("detailType", th.StringType),
            ),
        ),
        th.Property(
            "assetAccount",
            th.ObjectType(
                th.Property("id", th.IntegerType),
                th.Property("name", th.StringType),
                th.Property("number", th.StringType),
                th.Property("type", th.StringType),
                th.Property("detailType", th.StringType),
            ),
        ),
        th.Property("membershipTypeId", th.IntegerType),
        th.Property(
            "itemGroup",
            th.ObjectType(
                th.Property("rootId", th.IntegerType),
                th.Property("name", th.StringType),
            ),
        ),
        th.Property("displayName", th.StringType),
        th.Property("soldHours", th.NumberType),
        th.Property("modifiedOn", th.DateTimeType),
        th.Property("serviceDate", th.DateTimeType),
        th.Property("order", th.IntegerType),
        th.Property(
            "businessUnit",
            th.ObjectType(
                th.Property("id", th.IntegerType),
                th.Property("name", th.StringType),
            ),
        ),
        th.Property("active", th.BooleanType),
        th.Property("invoiceId", th.IntegerType),
    ).to_dict()

    @cached_property
    def path(self) -> str:
        """Return the API path for the stream."""
        return f"/accounting/v2/tenant/{self._tap.config['tenant_id']}/export/invoice-items"  # noqa: E501
```

# Example 3

URL path segment: /tenant/{tenant}/export/payments
JSON schema:

```
{'required': ['id', 'modifiedOn', 'createdOn', 'active'], 'type': 'object', 'properties': {'id': {'type': 'integer', 'format': 'int64'}, 'syncStatus': {'type': 'string', 'nullable': True}, 'referenceNumber': {'type': 'string', 'nullable': True}, 'date': {'type': 'string', 'format': 'date-time', 'nullable': True}, 'type': {'type': 'string', 'nullable': True}, 'typeId': {'type': 'string', 'nullable': True}, 'total': {'type': 'string', 'nullable': True}, 'unappliedAmount': {'type': 'string', 'nullable': True}, 'memo': {'type': 'string', 'nullable': True}, 'customer': {'oneOf': [{'required': ['id'], 'type': 'object', 'properties': {'id': {'type': 'integer', 'format': 'int64'}, 'name': {'type': 'string', 'nullable': True}}, 'additionalProperties': False}], 'nullable': True}, 'businessUnit': {'oneOf': [{'required': ['id'], 'type': 'object', 'properties': {'id': {'type': 'integer', 'format': 'int64'}, 'name': {'type': 'string', 'nullable': True}}, 'additionalProperties': False}], 'nullable': True}, 'batch': {'oneOf': [{'required': ['id'], 'type': 'object', 'properties': {'id': {'type': 'integer', 'format': 'int64'}, 'number': {'type': 'string', 'nullable': True}, 'name': {'type': 'string', 'nullable': True}}, 'additionalProperties': False}], 'nullable': True}, 'createdBy': {'type': 'string', 'nullable': True}, 'generalLedgerAccount': {'oneOf': [{'required': ['id'], 'type': 'object', 'properties': {'id': {'type': 'integer', 'format': 'int64'}, 'name': {'type': 'string', 'nullable': True}, 'number': {'type': 'string', 'nullable': True}, 'type': {'type': 'string', 'nullable': True}, 'detailType': {'type': 'string', 'nullable': True}}, 'additionalProperties': False}], 'nullable': True}, 'appliedTo': {'type': 'array', 'items': {'required': ['appliedId', 'appliedTo'], 'type': 'object', 'properties': {'appliedId': {'type': 'integer', 'format': 'int64'}, 'appliedTo': {'type': 'integer', 'format': 'int64'}, 'appliedAmount': {'type': 'string', 'nullable': True}, 'appliedOn': {'type': 'string', 'format': 'date-time', 'nullable': True}, 'appliedBy': {'type': 'string', 'nullable': True}, 'appliedToReferenceNumber': {'type': 'string', 'nullable': True}}, 'additionalProperties': False}, 'nullable': True}, 'customFields': {'type': 'array', 'items': {'required': ['name', 'value'], 'type': 'object', 'properties': {'name': {'type': 'string'}, 'value': {'type': 'string'}}, 'additionalProperties': False}, 'nullable': True}, 'authCode': {'type': 'string', 'nullable': True}, 'checkNumber': {'type': 'string', 'nullable': True}, 'modifiedOn': {'type': 'string', 'format': 'date-time'}, 'createdOn': {'type': 'string', 'format': 'date-time'}, 'active': {'type': 'boolean'}}, 'additionalProperties': False}
```

Resulting code:

```
class PaymentsStream(ServiceTitanExportStream):
    """Define payments stream."""

    name = "payments"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = th.PropertiesList(
        th.Property("id", th.IntegerType),
        th.Property("syncStatus", th.StringType),
        th.Property("referenceNumber", th.StringType),
        th.Property("date", th.DateTimeType),
        th.Property("type", th.StringType),
        th.Property("typeId", th.StringType),
        th.Property("total", th.StringType),
        th.Property("unappliedAmount", th.StringType),
        th.Property("memo", th.StringType),
        th.Property(
            "customer",
            th.ObjectType(
                th.Property("id", th.IntegerType), th.Property("name", th.StringType)
            ),
        ),
        th.Property(
            "businessUnit",
            th.ObjectType(
                th.Property("id", th.IntegerType), th.Property("name", th.StringType)
            ),
        ),
        th.Property(
            "batch",
            th.ObjectType(
                th.Property("id", th.IntegerType),
                th.Property("number", th.StringType),
                th.Property("name", th.StringType),
            ),
        ),
        th.Property("createdBy", th.StringType),
        th.Property(
            "generalLedgerAccount",
            th.ObjectType(
                th.Property("id", th.IntegerType),
                th.Property("name", th.StringType),
                th.Property("number", th.StringType),
                th.Property("type", th.StringType),
                th.Property("detailType", th.StringType),
            ),
        ),
        th.Property(
            "appliedTo",
            th.ArrayType(
                th.ObjectType(
                    th.Property("appliedId", th.IntegerType),
                    th.Property("appliedTo", th.IntegerType),
                    th.Property("appliedAmount", th.StringType),
                    th.Property("appliedOn", th.DateTimeType),
                    th.Property("appliedBy", th.StringType),
                    th.Property("appliedToReferenceNumber", th.StringType),
                )
            ),
        ),
        th.Property(
            "customFields",
            th.ArrayType(
                th.ObjectType(
                    th.Property("name", th.StringType),
                    th.Property("value", th.StringType),
                )
            ),
        ),
        th.Property("authCode", th.StringType),
        th.Property("checkNumber", th.StringType),
        th.Property("modifiedOn", th.DateTimeType),
        th.Property("createdOn", th.DateTimeType),
        th.Property("active", th.BooleanType),
    ).to_dict()

    @cached_property
    def path(self) -> str:
        """Return the API path for the stream."""
        return f"/accounting/v2/tenant/{self._tap.config['tenant_id']}/export/payments"
```

The URLs so far have been only for export endpoints. I'm going to provide you one example for a non-export endpoint.

# Non-export example

URL path: /tenant/{tenant}/estimates/items

JSON schema:

```
{'required': ['id', 'sku', 'skuAccount', 'description', 'qty', 'unitRate', 'total', 'unitCost', 'totalCost', 'itemGroupName', 'createdOn', 'modifiedOn'], 'type': 'object', 'properties': {'id': {'type': 'integer', 'format': 'int64'}, 'sku': {'required': ['id', 'name', 'displayName', 'type', 'soldHours', 'generalLedgerAccountId', 'generalLedgerAccountName', 'modifiedOn'], 'type': 'object', 'properties': {'id': {'type': 'integer', 'format': 'int64'}, 'name': {'type': 'string'}, 'displayName': {'type': 'string'}, 'type': {'type': 'string'}, 'soldHours': {'type': 'number', 'format': 'decimal'}, 'generalLedgerAccountId': {'type': 'integer', 'format': 'int64'}, 'generalLedgerAccountName': {'type': 'string'}, 'modifiedOn': {'type': 'string', 'format': 'date-time'}}, 'additionalProperties': False}, 'skuAccount': {'type': 'string'}, 'description': {'type': 'string', 'format': 'html'}, 'membershipTypeId': {'type': 'integer', 'format': 'int64', 'nullable': True}, 'qty': {'type': 'number', 'format': 'decimal'}, 'unitRate': {'type': 'number', 'format': 'decimal'}, 'total': {'type': 'number', 'format': 'decimal'}, 'unitCost': {'type': 'number', 'format': 'decimal'}, 'totalCost': {'type': 'number', 'format': 'decimal'}, 'itemGroupName': {'type': 'string'}, 'itemGroupRootId': {'type': 'integer', 'format': 'int64', 'nullable': True}, 'createdOn': {'type': 'string', 'format': 'date-time'}, 'modifiedOn': {'type': 'string', 'format': 'date-time'}, 'chargeable': {'type': 'boolean', 'nullable': True}}, 'additionalProperties': False}

```

Resulting code:

```
class EstimateItemsStream(ServiceTitanStream):
    """Define estimate items stream."""

    name = "estimate_items"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = th.PropertiesList(
        th.Property("id", th.IntegerType),
        th.Property(
            "sku",
            th.ObjectType(
                th.Property("id", th.IntegerType),
                th.Property("name", th.StringType),
                th.Property("displayName", th.StringType),
                th.Property("type", th.StringType),
                th.Property("soldHours", th.NumberType),
                th.Property("generalLedgerAccountId", th.IntegerType),
                th.Property("generalLedgerAccountName", th.StringType),
                th.Property("modifiedOn", th.DateTimeType),
            ),
        ),
        th.Property("skuAccount", th.StringType),
        th.Property("description", th.StringType),
        th.Property("membershipTypeId", th.IntegerType),
        th.Property("qty", th.NumberType),
        th.Property("unitRate", th.NumberType),
        th.Property("total", th.NumberType),
        th.Property("unitCost", th.NumberType),
        th.Property("totalCost", th.NumberType),
        th.Property("itemGroupName", th.StringType),
        th.Property("itemGroupRootId", th.IntegerType),
        th.Property("createdOn", th.DateTimeType),
        th.Property(
            "modifiedOn", th.DateTimeType
        ),  # Assuming datetime format as string
        th.Property("chargeable", th.BooleanType),
    ).to_dict()

    @cached_property
    def path(self) -> str:
        """Return the API path for the stream."""
        return f"/sales/v2/tenant/{self._tap.config['tenant_id']}/estimates/items"
```

All of the streams that have already been written are imported here:

```
"""ServiceTitan tap class."""

from __future__ import annotations

import typing as t

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_service_titan import streams

if t.TYPE_CHECKING:
    from tap_service_titan.client import ServiceTitanStream


class TapServiceTitan(Tap):
    """ServiceTitan tap class."""

    name = "tap-service-titan"

    dynamic_catalog: bool = True

    config_jsonschema = th.PropertiesList(
        th.Property(
            "client_id",
            th.StringType,
            required=True,
            secret=True,  # Flag config as protected.
            description="The client ID to use in authenticating.",
        ),
        th.Property(
            "client_secret",
            th.StringType,
            required=True,
            secret=True,  # Flag config as protected.
            description="The client secret to use in authenticating.",
        ),
        th.Property(
            "st_app_key",
            th.StringType,
            required=True,
            secret=True,  # Flag config as protected.
            description="The app key for the Service Titan app used to authenticate.",
        ),
        th.Property(
            "tenant_id",
            th.StringType,
            required=True,
            description="Tenant ID to pull records for.",
        ),
        th.Property(
            "api_url",
            th.StringType,
            default="https://api-integration.servicetitan.io",
            description="The url for the ServiceTitan API",
        ),
        th.Property(
            "auth_url",
            th.StringType,
            default="https://auth-integration.servicetitan.io/connect/token",
            description="The url for the ServiceTitan OAuth API",
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            default="2024-01-01T00:00:00Z",
            description="The start date for the records to pull.",
        ),
        th.Property(
            "custom_reports",
            th.ArrayType(
                th.ObjectType(
                    th.Property(
                        "report_category",
                        th.StringType,
                        required=True,
                        description="The category of the report to pull.",
                    ),
                    th.Property(
                        "report_name",
                        th.StringType,
                        required=True,
                        description="The name of the report to pull.",
                    ),
                    th.Property(
                        "report_id",
                        th.StringType,
                        required=True,
                        description="The ID of the report to pull.",
                    ),
                    th.Property(
                        "backfill_date_parameter",
                        th.StringType,
                        description="The date parameter to use for backfilling. The report will be retrieved for each date until the current date.",  # noqa: E501
                    ),
                    th.Property(
                        "parameters",
                        th.ArrayType(
                            th.ObjectType(
                                th.Property(
                                    "name",
                                    th.StringType,
                                    required=True,
                                    description="The name of the parameter.",
                                ),
                                th.Property(
                                    "value",
                                    th.StringType,
                                    required=True,
                                    description="The value of the parameter.",
                                ),
                            ),
                        ),
                        required=True,
                        description="The parameters to pass to the report.",
                    ),
                )
            ),
            description="Custom reports to extract.",
        ),
    ).to_dict()

    def discover_streams(self) -> list[ServiceTitanStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        streams_list = [
            streams.accounting.EstimateItemsStream(self),
            streams.accounting.InvoiceItemsStream(self),
            streams.accounting.InvoicesStream(self),
            streams.accounting.PaymentsStream(self),
            streams.accounting.InventoryBillsStream(self),
            streams.accounting.ApCreditsStream(self),
            streams.accounting.ApPaymentsStream(self),
            streams.accounting.PaymentTermsStream(self),
            streams.accounting.PaymentTypesStream(self),
            streams.accounting.TaxZonesStream(self),
            streams.accounting.JournalEntriesStream(self),
            streams.crm.BookingProviderTagsStream(self),
            streams.crm.BookingsStream(self),
            streams.crm.CustomerContactsStream(self),
            streams.crm.CustomersStream(self),
            streams.crm.LeadsStream(self),
            streams.crm.LocationContactsStream(self),
            streams.crm.LocationsStream(self),
            streams.dispatch.CapacitiesStream(self),
            streams.dispatch.AppointmentAssignmentsStream(self),
            streams.dispatch.ArrivalWindowsStream(self),
            streams.dispatch.NonJobAppointmentsStream(self),
            streams.dispatch.TeamsStream(self),
            streams.dispatch.TechnicianShiftsStream(self),
            streams.dispatch.ZonesStream(self),
            streams.equipment_systems.InstalledEquipmentStream(self),
            streams.forms.FormsStream(self),
            streams.forms.SubmissionsStream(self),
            streams.inventory.AdjustmentsStream(self),
            streams.inventory.ReturnTypesStream(self),
            streams.inventory.TransfersStream(self),
            streams.inventory.TrucksStream(self),
            streams.inventory.VendorsStream(self),
            streams.inventory.WarehousesStream(self),
            streams.inventory.PurchaseOrderMarkupsStream(self),
            streams.inventory.PurchaseOrdersStream(self),
            streams.inventory.PurchaseOrderTypesStream(self),
            streams.inventory.ReceiptsStream(self),
            streams.inventory.ReturnsStream(self),
            streams.job_booking.CallReasonsStream(self),
            streams.jpm.AppointmentsStream(self),
            streams.jpm.JobCancelReasonsStream(self),
            streams.jpm.JobCancelledLogsStream(self),
            streams.jpm.JobHistoryStream(self),
            streams.jpm.JobHoldReasonsStream(self),
            streams.jpm.JobsStream(self),
            streams.jpm.JobTypesStream(self),
            streams.jpm.ProjectStatusesStream(self),
            streams.jpm.ProjectSubStatusesStream(self),
            streams.jpm.ProjectsStream(self),
            streams.marketing.CampaignsStream(self),
            streams.marketing.CategoriesStream(self),
            streams.marketing.CostsStream(self),
            streams.marketing.SuppressionsStream(self),
            streams.marketing_reputation.ReviewsStream(self),
            streams.sales_and_estimates.EstimatesStream(self),
            streams.service_agreements.ServiceAgreementsStream(self),
            streams.settings.BusinessUnitsStream(self),
            streams.settings.EmployeesStream(self),
            streams.settings.TechniciansStream(self),
            streams.telecom.CallsStream(self),
        ]
        custom_reports_config = self.config.get("custom_reports", [])
        if custom_reports_config:
            streams_list.extend(
                [
                    streams.reporting.CustomReports(self, report=report)
                    for report in custom_reports_config
                ]
            )
        return streams_list


if __name__ == "__main__":
    TapServiceTitan.cli()
```

Here's the list of already implemented streams in plain text:

```
"adjustments"
"ap_credits"
"ap_payments"
"appointment_assignments"
"appointments"
"arrival_windows"
"booking_provider_tags"
"bookings"
"business_units"
"call_reasons"
"calls"
"campaigns"
"capacities"
"categories"
"costs"
"customer_contacts"
"customers"
"employees"
"estimate_items"
"estimates"
"forms"
"installed_equipment"
"inventory_bills"
"invoice_items"
"invoices"
"job_cancel_reasons"
"job_canceled_logs"
"job_history"
"job_hold_reasons"
"job_types"
"jobs"
"journal_entries"
"leads"
"location_contacts"
"locations"
"non_job_appointments"
"payment_terms"
"payment_types"
"payments"
"project_statuses"
"project_sub_statuses"
"projects"
"purchase_order_markups"
"purchase_order_types"
"purchase_orders"
"receipts"
"return_types"
"returns"
"reviews"
"service_agreements"
"submissions"
"suppressions"
"tax_zones"
"teams"
"technician_shifts"
"technicians"
"transfers"
"trucks"
"vendors"
"warehouses"
"zones"
```

If I ask you to write code for a stream that's already been implemented, just say "already implemented" instead of returning code. For example, if I were to give you this:

```
This is for the accounting v2 API.
URL path segment: /tenant/{tenant}/ap-credits
JSON schema:
{'required': ['id', 'active', 'createdOn', 'modifiedOn', 'date', 'amount', 'appliedAmount', 'status', 'syncStatus', 'paymentStatus', 'splits'], 'type': 'object', 'properties': {'id': {'type': 'integer', 'format': 'int64'}, 'inventoryReturnId': {'type': 'integer', 'format': 'int64', 'nullable': True}, 'jobId': {'type': 'integer', 'format': 'int64', 'nullable': True}, 'active': {'type': 'boolean'}, 'createdOn': {'type': 'string', 'format': 'date-time'}, 'modifiedOn': {'type': 'string', 'format': 'date-time'}, 'date': {'type': 'string', 'format': 'date-time'}, 'canceledOn': {'type': 'string', 'format': 'date-time', 'nullable': True}, 'number': {'type': 'string', 'nullable': True}, 'referenceNumber': {'type': 'string', 'nullable': True}, 'memo': {'type': 'string', 'nullable': True}, 'amount': {'type': 'number', 'format': 'decimal'}, 'appliedAmount': {'type': 'number', 'format': 'decimal'}, 'status': {'enum': ['Unreconciled', 'Canceled'], 'type': 'string', 'description': '', 'x-enumNames': ['Unreconciled', 'Canceled']}, 'syncStatus': {'enum': ['Pending', 'Posted', 'Exported'], 'type': 'string', 'description': '', 'x-enumNames': ['Pending', 'Posted', 'Exported']}, 'batch': {'oneOf': [{'required': ['id'], 'type': 'object', 'properties': {'id': {'type': 'integer', 'format': 'int64'}, 'number': {'type': 'string', 'nullable': True}, 'name': {'type': 'string', 'nullable': True}}, 'additionalProperties': False}], 'nullable': True}, 'businessUnit': {'oneOf': [{'required': ['id'], 'type': 'object', 'properties': {'id': {'type': 'integer', 'format': 'int64'}, 'name': {'type': 'string', 'nullable': True}}, 'additionalProperties': False}], 'nullable': True}, 'remittanceVendor': {'oneOf': [{'required': ['id'], 'type': 'object', 'properties': {'id': {'type': 'integer', 'format': 'int64'}, 'name': {'type': 'string', 'nullable': True}}, 'additionalProperties': False}], 'nullable': True}, 'vendor': {'oneOf': [{'required': ['id'], 'type': 'object', 'properties': {'id': {'type': 'integer', 'format': 'int64'}, 'name': {'type': 'string', 'nullable': True}}, 'additionalProperties': False}], 'nullable': True}, 'paymentStatus': {'enum': ['Unpaid', 'ReadyForApproval', 'Initiated', 'Paid', 'Cancelled', 'InTransit', 'Processing'], 'type': 'string', 'description': '', 'x-enumNames': ['Unpaid', 'ReadyForApproval', 'Initiated', 'Paid', 'Cancelled', 'InTransit', 'Processing']}, 'splits': {'type': 'array', 'items': {'required': ['id', 'active', 'createdOn', 'inventoryBillId', 'vendorCreditId', 'amount'], 'type': 'object', 'properties': {'id': {'type': 'integer', 'format': 'int64'}, 'active': {'type': 'boolean'}, 'createdOn': {'type': 'string', 'format': 'date-time'}, 'inventoryBillId': {'type': 'integer', 'format': 'int64'}, 'vendorCreditId': {'type': 'integer', 'format': 'int64'}, 'amount': {'type': 'number', 'format': 'decimal'}}, 'additionalProperties': False}}}, 'additionalProperties': False}
```

You'd see that the `streams.accounting.ApPaymentsStream(self)` is already listed in `streams_list` and you would just say "already implemented"

Alright, now I'll be providing url paths and json schemas and expecting you to write the tap stream. Please ask questions if needed but otherwise exclusively output the code with no commentary.
