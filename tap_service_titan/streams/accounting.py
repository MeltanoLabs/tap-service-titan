"""Accounting streams for the ServiceTitan tap."""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING

from tap_service_titan.client import ServiceTitanExportStream, ServiceTitanStream
from tap_service_titan.openapi_specs import ACCOUNTING, ServiceTitanSchema

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

if TYPE_CHECKING:
    from collections.abc import Iterable

    from singer_sdk.helpers.types import Context, Record


class _BaseAccountingStream(ServiceTitanStream, api_prefix="/accounting/v2"):
    pass


class _BaseAccountingExportStream(ServiceTitanExportStream, api_prefix="/accounting/v2"):
    pass


class InvoicesStream(_BaseAccountingExportStream):
    """Define invoices stream."""

    name = "invoices"
    path = "/export/invoices"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(ACCOUNTING, key="Accounting.V2.ExportInvoiceResponse")


class InvoiceItemsStream(_BaseAccountingExportStream):
    """Define invoice items stream."""

    name = "invoice_items"
    path = "/export/invoice-items"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(ACCOUNTING, key="Accounting.V2.ExportInvoiceItemResponse")


class PaymentsStream(_BaseAccountingExportStream):
    """Define payments stream."""

    name = "payments"
    path = "/export/payments"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(ACCOUNTING, key="Accounting.V2.ExportPaymentResponse")


class InventoryBillsStream(_BaseAccountingExportStream):
    """Define inventory bills stream."""

    name = "inventory_bills"
    path = "/export/inventory-bills"
    primary_keys = ("id",)
    replication_key: str = "createdOn"
    schema = ServiceTitanSchema(ACCOUNTING, key="Accounting.V2.ExportInventoryBillResponse")


class ApCreditsStream(_BaseAccountingStream):
    """Define ap credits stream."""

    name = "ap_credits"
    path = "/ap-credits"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(ACCOUNTING, key="Accounting.V2.ApCreditResponse")


class ApPaymentsStream(_BaseAccountingStream):
    """Define ap payment stream."""

    name = "ap_payments"
    path = "/ap-payments"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(ACCOUNTING, key="Accounting.V2.ApPaymentResponse")


class PaymentTermsStream(_BaseAccountingStream):
    """Define payment terms stream."""

    name = "payment_terms"
    path = "/payment-terms"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(ACCOUNTING, key="Accounting.V2.PaymentTermAPIModel")


class PaymentTypesStream(_BaseAccountingStream):
    """Define payment types stream."""

    name = "payment_types"
    path = "/payment-types"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(ACCOUNTING, key="Accounting.V2.PaymentTypeResponse")


class TaxZonesStream(_BaseAccountingStream):
    """Define tax zones stream."""

    name = "tax_zones"
    path = "/tax-zones"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(ACCOUNTING, key="Accounting.V2.TaxZoneResponse")


class JournalEntriesStream(_BaseAccountingStream, page_size=500):
    """Define journal entries stream."""

    name = "journal_entries"
    path = "/journal-entries"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(ACCOUNTING, key="Accounting.V2.JournalEntryResponse")

    @override
    def get_child_context(self, record: Record, context: Context | None) -> Context:
        """Return a context dictionary for a child stream."""
        return {"journal_entry_id": record["id"]}


class JournalEntrySummaryStream(_BaseAccountingStream, page_size=500):
    """Define journal entry summary stream."""

    name = "journal_entry_summaries"
    path = "/journal-entries/{journal_entry_id}/summary"
    primary_keys = ()
    replication_key: str | None = None
    parent_stream_type = JournalEntriesStream
    ignore_parent_replication_key = True
    schema = ServiceTitanSchema(ACCOUNTING, key="Accounting.V2.JournalEntrySummaryResponse")


class JournalEntryDetailsStream(_BaseAccountingStream, page_size=500):
    """Define journal entry details stream."""

    name = "journal_entry_details"
    path = "/journal-entries/{journal_entry_id}/details"
    primary_keys = ()
    replication_key: str | None = None
    parent_stream_type = JournalEntriesStream
    ignore_parent_replication_key = True
    schema = ServiceTitanSchema(ACCOUNTING, key="Accounting.V2.JournalEntryDetailsResponse")


class InventoryBillsCustomFieldsStream(_BaseAccountingStream):
    """Define inventory bills custom fields stream."""

    name = "inventory_bills_custom_fields"
    path = "/inventory-bills/custom-fields"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(ACCOUNTING, key="Accounting.V2.CustomFieldTypeResponse")


class GLAccountsStream(_BaseAccountingStream):
    """Define GL accounts stream."""

    name = "gl_accounts"
    path = "/gl-accounts"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(ACCOUNTING, key="Accounting.V2.GlAccountExtendedResponse")


class GLAccountTypesStream(_BaseAccountingStream):
    """Define GL account types stream."""

    name = "gl_account_types"
    path = "/gl-accounts/types"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(ACCOUNTING, key="Accounting.V2.GlAccountTypeResponse")


class CreditMemosStream(_BaseAccountingStream):
    """Define credit memos stream."""

    name = "credit_memos"
    path = "/credit-memos"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(
        ACCOUNTING,
        key="Accounting.V2.CreditMemos.CreditMemoPublicResponse",
    )


class BankDepositsStream(_BaseAccountingStream):
    """Define bank deposits stream."""

    name = "bank_deposits"
    path = "/bank-deposits"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(ACCOUNTING, key="Accounting.V2.DetailedDepositResponse")

    @override
    def generate_child_contexts(
        self, record: Record, context: Context | None
    ) -> Iterable[Context | None]:
        yield {"bankDepositId": record["id"]}


class BankDepositTransactionsStream(_BaseAccountingStream):
    """Define bank deposit transactions stream."""

    name = "bank_deposit_transactions"
    path = "/bank-deposits/{bankDepositId}/transactions"
    primary_keys = ("id",)
    replication_key = None
    parent_stream_type = BankDepositsStream
    schema = ServiceTitanSchema(ACCOUNTING, key="Accounting.V2.DetailedTransactionResponse")
