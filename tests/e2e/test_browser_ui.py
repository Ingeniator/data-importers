"""
Playwright E2E tests for the dataimporter browser UI.

Run against the dev server:
    make dev                           # starts on :5001
    uv run pytest tests/e2e/ -v

Run against a specific URL:
    DATAIMPORTER_URL=http://host:8888/dataimporter uv run pytest tests/e2e/ -v

Tests marked xfail require a live backend (real datasource, real credentials).
Tests without a mark should pass against any running server instance.
"""

import json

import pytest
from playwright.sync_api import Page, Route, expect

from tests.e2e.conftest import MOCK_UI_CONFIG


# ─────────────────────────────────────────────────────────────────────────────
# App Structure — nav, page shell
# ─────────────────────────────────────────────────────────────────────────────


class TestNavigation:
    def test_page_loads(self, ui_page: Page) -> None:
        expect(ui_page).to_have_title("dataimporter")

    def test_top_nav_present(self, ui_page: Page) -> None:
        expect(ui_page.get_by_test_id("top-nav")).to_be_visible()

    def test_nav_datasources_link(self, ui_page: Page) -> None:
        link = ui_page.get_by_test_id("nav-datasources")
        expect(link).to_be_visible()
        expect(link).to_have_text("Datasources")

    def test_nav_connections_link(self, ui_page: Page) -> None:
        link = ui_page.get_by_test_id("nav-connections")
        expect(link).to_be_visible()
        expect(link).to_have_text("Connections")

    def test_nav_jobs_link(self, ui_page: Page) -> None:
        link = ui_page.get_by_test_id("nav-jobs")
        expect(link).to_be_visible()

    def test_nav_jobs_badge_hidden_initially(self, ui_page: Page) -> None:
        badge = ui_page.get_by_test_id("nav-jobs-badge")
        expect(badge).to_be_hidden()

    def test_auth_card_visible(self, ui_page: Page) -> None:
        expect(ui_page.locator("#authCard")).to_be_visible()

    def test_auth_pk_input_present(self, ui_page: Page) -> None:
        expect(ui_page.locator("#pk")).to_be_visible()
        expect(ui_page.locator("#pk")).to_have_attribute("placeholder", "pk-...")

    def test_auth_sk_input_present(self, ui_page: Page) -> None:
        expect(ui_page.locator("#sk")).to_be_visible()
        expect(ui_page.locator("#sk")).to_have_attribute("placeholder", "sk-...")

    def test_filters_card_hidden_on_load(self, ui_page: Page) -> None:
        expect(ui_page.locator("#filtersCard")).to_be_hidden()

    def test_export_card_hidden_on_load(self, ui_page: Page) -> None:
        expect(ui_page.locator("#exportCard")).to_be_hidden()

    def test_results_hidden_on_load(self, ui_page: Page) -> None:
        expect(ui_page.locator("#results")).to_be_hidden()


# ─────────────────────────────────────────────────────────────────────────────
# P1-S1: Datasource tabs (requires mocked ui-config)
# ─────────────────────────────────────────────────────────────────────────────


class TestDatasourceTabs:
    def test_tabs_container_present(self, ui_page_with_config: Page) -> None:
        expect(ui_page_with_config.locator("#dsTabs")).to_be_visible()

    def test_tabs_rendered_from_config(self, ui_page_with_config: Page) -> None:
        page = ui_page_with_config
        # Expect one tab per datasource in MOCK_UI_CONFIG
        tabs = page.locator("#dsTabs .ds-tab")
        expect(tabs).to_have_count(len(MOCK_UI_CONFIG["datasources"]) + 1)  # +1 for "Add" tab

    def test_s3_tab_present(self, ui_page_with_config: Page) -> None:
        page = ui_page_with_config
        s3_tab = page.locator("#dsTabs .ds-tab").filter(has_text="s3-prod")
        expect(s3_tab).to_be_visible()

    def test_clickhouse_tab_present(self, ui_page_with_config: Page) -> None:
        page = ui_page_with_config
        ch_tab = page.locator("#dsTabs .ds-tab").filter(has_text="clickhouse-prod")
        expect(ch_tab).to_be_visible()

    def test_selecting_tab_reveals_filters_card(self, ui_page_with_config: Page) -> None:
        page = ui_page_with_config
        first_tab = page.locator("#dsTabs .ds-tab").first
        first_tab.click()
        expect(page.locator("#filtersCard")).to_be_visible()

    def test_selecting_tab_reveals_export_card(self, ui_page_with_config: Page) -> None:
        page = ui_page_with_config
        first_tab = page.locator("#dsTabs .ds-tab").first
        first_tab.click()
        expect(page.locator("#exportCard")).to_be_visible()


# ─────────────────────────────────────────────────────────────────────────────
# P1-S2: Time range picker
# ─────────────────────────────────────────────────────────────────────────────


class TestTimeRangePicker:
    @pytest.fixture(autouse=True)
    def open_datasource(self, ui_page_with_config: Page) -> None:
        self.page = ui_page_with_config
        self.page.locator("#dsTabs .ds-tab").first.click()

    def test_time_range_btn_visible(self) -> None:
        expect(self.page.get_by_test_id("time-range-btn")).to_be_visible()

    def test_dropdown_hidden_initially(self) -> None:
        expect(self.page.get_by_test_id("time-range-dropdown")).to_be_hidden()

    def test_click_opens_dropdown(self) -> None:
        self.page.get_by_test_id("time-range-btn").click()
        expect(self.page.get_by_test_id("time-range-dropdown")).to_be_visible()

    def test_preset_15m_present(self) -> None:
        self.page.get_by_test_id("time-range-btn").click()
        expect(self.page.get_by_test_id("time-preset-15m")).to_be_visible()

    def test_preset_all_present(self) -> None:
        self.page.get_by_test_id("time-range-btn").click()
        expect(self.page.get_by_test_id("time-preset-all")).to_be_visible()

    def test_selecting_preset_updates_label(self) -> None:
        self.page.get_by_test_id("time-range-btn").click()
        self.page.get_by_test_id("time-preset-7d").click()
        expect(self.page.locator("#timeLabel")).to_have_text("Last 7 days")

    def test_selecting_preset_closes_dropdown(self) -> None:
        self.page.get_by_test_id("time-range-btn").click()
        self.page.get_by_test_id("time-preset-1h").click()
        expect(self.page.get_by_test_id("time-range-dropdown")).to_be_hidden()

    def test_custom_from_input_present(self) -> None:
        self.page.get_by_test_id("time-range-btn").click()
        expect(self.page.get_by_test_id("time-from")).to_be_visible()

    def test_custom_to_input_present(self) -> None:
        self.page.get_by_test_id("time-range-btn").click()
        expect(self.page.get_by_test_id("time-to")).to_be_visible()

    def test_apply_custom_range_button_present(self) -> None:
        self.page.get_by_test_id("time-range-btn").click()
        expect(self.page.get_by_test_id("apply-time-range-btn")).to_be_visible()

    def test_apply_custom_range(self) -> None:
        self.page.get_by_test_id("time-range-btn").click()
        self.page.get_by_test_id("time-from").fill("2024-01-01T00:00")
        self.page.get_by_test_id("time-to").fill("2024-01-31T23:59")
        self.page.get_by_test_id("apply-time-range-btn").click()
        expect(self.page.get_by_test_id("time-range-dropdown")).to_be_hidden()

    def test_click_outside_closes_dropdown(self) -> None:
        self.page.get_by_test_id("time-range-btn").click()
        expect(self.page.get_by_test_id("time-range-dropdown")).to_be_visible()
        self.page.locator("h3", has_text="Refine Dataset").click()
        expect(self.page.get_by_test_id("time-range-dropdown")).to_be_hidden()


# ─────────────────────────────────────────────────────────────────────────────
# P1-S2: Search input
# ─────────────────────────────────────────────────────────────────────────────


class TestSearchInput:
    @pytest.fixture(autouse=True)
    def open_datasource(self, ui_page_with_config: Page) -> None:
        self.page = ui_page_with_config
        self.page.locator("#dsTabs .ds-tab").first.click()

    def test_search_input_visible(self) -> None:
        expect(self.page.get_by_test_id("search-input")).to_be_visible()

    def test_search_input_accepts_text(self) -> None:
        self.page.get_by_test_id("search-input").fill("error trace")
        expect(self.page.get_by_test_id("search-input")).to_have_value("error trace")

    def test_search_input_clearable(self) -> None:
        inp = self.page.get_by_test_id("search-input")
        inp.fill("something")
        inp.fill("")
        expect(inp).to_have_value("")

    @pytest.mark.xfail(reason="Requires live backend with a configured datasource")
    def test_search_returns_results(self) -> None:
        self.page.get_by_test_id("search-input").fill("hello")
        self.page.keyboard.press("Enter")
        expect(self.page.get_by_test_id("results-table")).to_be_visible()


# ─────────────────────────────────────────────────────────────────────────────
# P1-S3: Field filter panel
# ─────────────────────────────────────────────────────────────────────────────


class TestFilterPanel:
    @pytest.fixture(autouse=True)
    def open_datasource(self, ui_page_with_config: Page) -> None:
        self.page = ui_page_with_config
        # Pick a search-capable datasource so the filter toggle is visible
        self.page.locator("#dsTabs .ds-tab").nth(1).click()

    def test_filter_toggle_btn_visible(self) -> None:
        expect(self.page.get_by_test_id("filter-toggle-btn")).to_be_visible()

    def test_filter_panel_hidden_initially(self) -> None:
        expect(self.page.get_by_test_id("filter-panel")).to_be_hidden()

    def test_toggle_opens_filter_panel(self) -> None:
        self.page.get_by_test_id("filter-toggle-btn").click()
        expect(self.page.get_by_test_id("filter-panel")).to_be_visible()

    def test_toggle_closes_filter_panel(self) -> None:
        self.page.get_by_test_id("filter-toggle-btn").click()
        self.page.get_by_test_id("filter-toggle-btn").click()
        expect(self.page.get_by_test_id("filter-panel")).to_be_hidden()

    def test_filter_mode_buttons_present(self) -> None:
        self.page.get_by_test_id("filter-toggle-btn").click()
        expect(self.page.get_by_test_id("filter-mode-and")).to_be_visible()
        expect(self.page.get_by_test_id("filter-mode-or")).to_be_visible()

    def test_filter_field_selector_present(self) -> None:
        self.page.get_by_test_id("filter-toggle-btn").click()
        expect(self.page.get_by_test_id("filter-field-sel")).to_be_visible()

    def test_filter_op_selector_present(self) -> None:
        self.page.get_by_test_id("filter-toggle-btn").click()
        expect(self.page.get_by_test_id("filter-op-sel")).to_be_visible()

    def test_filter_op_selector_has_operators(self) -> None:
        self.page.get_by_test_id("filter-toggle-btn").click()
        sel = self.page.get_by_test_id("filter-op-sel")
        options = sel.locator("option").all_text_contents()
        assert "contains" in options
        assert "equals" in options
        assert "is empty" in options

    def test_filter_value_input_present(self) -> None:
        self.page.get_by_test_id("filter-toggle-btn").click()
        expect(self.page.get_by_test_id("filter-val-input")).to_be_visible()

    def test_filter_add_btn_present(self) -> None:
        self.page.get_by_test_id("filter-toggle-btn").click()
        expect(self.page.get_by_test_id("filter-add-btn")).to_be_visible()

    def test_filter_mode_and_active_by_default(self) -> None:
        self.page.get_by_test_id("filter-toggle-btn").click()
        and_btn = self.page.get_by_test_id("filter-mode-and")
        expect(and_btn).to_have_class(lambda cls: "active" in cls)

    def test_switch_to_or_mode(self) -> None:
        self.page.get_by_test_id("filter-toggle-btn").click()
        self.page.get_by_test_id("filter-mode-or").click()
        expect(self.page.get_by_test_id("filter-mode-or")).to_have_class(lambda cls: "active" in cls)

    @pytest.mark.xfail(reason="filter_field_sel is populated from schema discovery which needs a live backend")
    def test_add_filter_rule_creates_chip(self) -> None:
        self.page.get_by_test_id("filter-toggle-btn").click()
        self.page.get_by_test_id("filter-field-sel").select_option(index=1)
        self.page.get_by_test_id("filter-val-input").fill("error")
        self.page.get_by_test_id("filter-add-btn").click()
        expect(self.page.locator("#filter_rules .filter-rule")).to_have_count(1)


# ─────────────────────────────────────────────────────────────────────────────
# P1-S4: Column picker panel
# ─────────────────────────────────────────────────────────────────────────────


class TestColumnPicker:
    @pytest.fixture(autouse=True)
    def open_datasource(self, ui_page_with_config: Page) -> None:
        self.page = ui_page_with_config
        self.page.locator("#dsTabs .ds-tab").first.click()

    def test_column_picker_panel_hidden_initially(self) -> None:
        expect(self.page.get_by_test_id("column-picker-panel")).to_be_hidden()

    @pytest.mark.xfail(reason="Column picker toggle button is rendered dynamically by JS after schema loads")
    def test_column_picker_toggle_visible(self) -> None:
        expect(self.page.get_by_test_id("column-picker-btn")).to_be_visible()

    @pytest.mark.xfail(reason="Requires schema discovery against a live backend")
    def test_column_picker_shows_after_schema_load(self) -> None:
        expect(self.page.get_by_test_id("column-picker-panel")).to_be_visible()


# ─────────────────────────────────────────────────────────────────────────────
# P1-S5: Event preview modal
# ─────────────────────────────────────────────────────────────────────────────


class TestEventPreview:
    @pytest.mark.xfail(reason="Preview modal is rendered dynamically by JS — requires result rows to be present")
    def test_preview_modal_accessible(self, ui_page_with_search: Page) -> None:
        page = ui_page_with_search
        page.locator("#dsTabs .ds-tab").nth(1).click()
        page.get_by_test_id("search-input").fill("hello")
        page.keyboard.press("Enter")
        page.wait_for_timeout(500)
        page.locator("[data-testid^='result-row-']").first.click()
        expect(page.get_by_test_id("preview-modal")).to_be_visible()

    @pytest.mark.xfail(reason="Requires live result rows")
    def test_preview_close_btn_closes_modal(self, ui_page_with_search: Page) -> None:
        page = ui_page_with_search
        page.locator("#dsTabs .ds-tab").nth(1).click()
        page.get_by_test_id("search-input").fill("hello")
        page.keyboard.press("Enter")
        page.wait_for_timeout(500)
        page.locator("[data-testid^='result-row-']").first.click()
        page.get_by_test_id("preview-close-btn").click()
        expect(page.get_by_test_id("preview-modal")).to_be_hidden()


# ─────────────────────────────────────────────────────────────────────────────
# P1-S6: Export modal — destination selectors
# ─────────────────────────────────────────────────────────────────────────────


class TestExportDestination:
    @pytest.fixture(autouse=True)
    def open_datasource(self, ui_page_with_config: Page) -> None:
        self.page = ui_page_with_config
        self.page.locator("#dsTabs .ds-tab").first.click()

    def test_dest_dataset_name_present(self) -> None:
        expect(self.page.get_by_test_id("dest-dataset-name")).to_be_visible()

    def test_dest_dataset_name_accepts_input(self) -> None:
        inp = self.page.get_by_test_id("dest-dataset-name")
        inp.fill("my-test-export")
        expect(inp).to_have_value("my-test-export")

    def test_dest_target_populated_from_config(self) -> None:
        sel = self.page.get_by_test_id("dest-target")
        options = sel.locator("option").all_text_contents()
        assert any("langfuse-prod" in o for o in options)

    def test_dest_access_has_options(self) -> None:
        sel = self.page.get_by_test_id("dest-access")
        options = sel.locator("option").all_text_contents()
        assert "organization" in options
        assert "private" in options

    def test_dest_dataset_type_has_options(self) -> None:
        sel = self.page.get_by_test_id("dest-dataset-type")
        options = sel.locator("option").all_text_contents()
        assert "DATASET" in options
        assert "EXPERIMENT" in options

    @pytest.mark.xfail(reason="Export modal rendered by JS — requires destination target to be selected")
    def test_export_modal_opens(self) -> None:
        self.page.get_by_test_id("dest-target").select_option(index=1)
        self.page.get_by_test_id("dest-dataset-name").fill("test-export")
        self.page.locator("button", has_text="Export").click()
        expect(self.page.get_by_test_id("export-modal")).to_be_visible()

    def test_yaml_file_input_present(self) -> None:
        inp = self.page.get_by_test_id("yaml-file-input")
        expect(inp).to_have_attribute("accept", ".yaml,.yml")

    def test_export_col_mask_checkbox_unchecked_by_default(self) -> None:
        cb = self.page.get_by_test_id("export-col-mask-checkbox")
        expect(cb).not_to_be_checked()


# ─────────────────────────────────────────────────────────────────────────────
# P1-S7: Sampling panel
# ─────────────────────────────────────────────────────────────────────────────


class TestSamplingPanel:
    @pytest.fixture(autouse=True)
    def open_datasource(self, ui_page_with_config: Page) -> None:
        self.page = ui_page_with_config
        self.page.locator("#dsTabs .ds-tab").first.click()

    def test_sampling_toggle_btn_visible(self) -> None:
        expect(self.page.get_by_test_id("sampling-toggle-btn")).to_be_visible()

    def test_sampling_panel_hidden_initially(self) -> None:
        expect(self.page.get_by_test_id("sampling-panel")).to_be_hidden()

    def test_toggle_opens_sampling_panel(self) -> None:
        self.page.get_by_test_id("sampling-toggle-btn").click()
        expect(self.page.get_by_test_id("sampling-panel")).to_be_visible()

    def test_toggle_closes_sampling_panel(self) -> None:
        self.page.get_by_test_id("sampling-toggle-btn").click()
        self.page.get_by_test_id("sampling-toggle-btn").click()
        expect(self.page.get_by_test_id("sampling-panel")).to_be_hidden()

    def test_sampling_strategy_selector_present(self) -> None:
        self.page.get_by_test_id("sampling-toggle-btn").click()
        # Strategy selector only renders after schema loads
        expect(self.page.get_by_test_id("sampling-strategy-sel")).to_be_attached()

    def test_sampling_add_strategy_btn_present(self) -> None:
        self.page.get_by_test_id("sampling-toggle-btn").click()
        expect(self.page.get_by_test_id("sampling-add-strategy-btn")).to_be_attached()

    @pytest.mark.xfail(reason="samp_options only shows after schema discovery loads — needs live backend")
    def test_strict_checkbox_unchecked_by_default(self) -> None:
        self.page.get_by_test_id("sampling-toggle-btn").click()
        expect(self.page.get_by_test_id("sampling-strict-checkbox")).not_to_be_checked()

    @pytest.mark.xfail(reason="samp_options only shows after schema discovery loads — needs live backend")
    def test_max_traces_input_empty_by_default(self) -> None:
        self.page.get_by_test_id("sampling-toggle-btn").click()
        expect(self.page.get_by_test_id("sampling-max-traces")).to_have_value("")

    @pytest.mark.xfail(reason="samp_options only shows after schema discovery loads — needs live backend")
    def test_max_traces_accepts_number(self) -> None:
        self.page.get_by_test_id("sampling-toggle-btn").click()
        self.page.get_by_test_id("sampling-max-traces").fill("5000")
        expect(self.page.get_by_test_id("sampling-max-traces")).to_have_value("5000")

    @pytest.mark.xfail(reason="yield estimate only appears after a strategy is added with a live schema")
    def test_yield_estimate_visible_after_strategy_added(self) -> None:
        expect(self.page.get_by_test_id("sampling-yield-estimate")).to_be_visible()


# ─────────────────────────────────────────────────────────────────────────────
# Scheduling & Automation — schedule + webhook
# ─────────────────────────────────────────────────────────────────────────────


class TestSchedulingAutomation:
    @pytest.fixture(autouse=True)
    def open_datasource(self, ui_page_with_config: Page) -> None:
        self.page = ui_page_with_config
        self.page.locator("#dsTabs .ds-tab").first.click()

    def test_schedule_cron_input_present(self) -> None:
        expect(self.page.locator("#schedule_cron")).to_be_visible()

    def test_schedule_cron_default_empty(self) -> None:
        expect(self.page.locator("#schedule_cron")).to_have_value("")

    def test_schedule_cron_accepts_expression(self) -> None:
        self.page.locator("#schedule_cron").fill("0 9 * * 1")
        expect(self.page.locator("#schedule_cron")).to_have_value("0 9 * * 1")

    def test_schedule_tz_defaults_to_utc(self) -> None:
        expect(self.page.locator("#schedule_tz")).to_have_value("UTC")

    def test_schedule_enabled_checkbox_checked_by_default(self) -> None:
        expect(self.page.locator("#schedule_enabled")).to_be_checked()

    def test_schedule_enabled_toggle(self) -> None:
        cb = self.page.locator("#schedule_enabled")
        cb.uncheck()
        expect(cb).not_to_be_checked()
        cb.check()
        expect(cb).to_be_checked()

    def test_webhook_url_input_present(self) -> None:
        expect(self.page.locator("#webhook_url")).to_be_visible()

    def test_webhook_url_accepts_input(self) -> None:
        self.page.locator("#webhook_url").fill("https://example.com/hook")
        expect(self.page.locator("#webhook_url")).to_have_value("https://example.com/hook")

    def test_webhook_secret_input_present(self) -> None:
        expect(self.page.locator("#webhook_secret")).to_be_visible()

    def test_webhook_timeout_defaults_to_30(self) -> None:
        expect(self.page.locator("#webhook_timeout")).to_have_value("30")

    def test_webhook_headers_textarea_present(self) -> None:
        expect(self.page.locator("#webhook_headers")).to_be_visible()


# ─────────────────────────────────────────────────────────────────────────────
# Data Masking
# ─────────────────────────────────────────────────────────────────────────────


class TestDataMasking:
    @pytest.fixture(autouse=True)
    def open_datasource(self, ui_page_with_config: Page) -> None:
        self.page = ui_page_with_config
        self.page.locator("#dsTabs .ds-tab").first.click()

    def test_mask_rules_section_present(self) -> None:
        expect(self.page.locator("#mask_rules_section")).to_be_visible()

    def test_mask_field_input_accepts_path(self) -> None:
        self.page.locator("#mask_field_inp").fill("user.email")
        expect(self.page.locator("#mask_field_inp")).to_have_value("user.email")

    def test_mask_action_selector_options(self) -> None:
        sel = self.page.locator("#mask_action_sel")
        options = sel.locator("option").all_text_contents()
        assert "remove" in options
        assert "hash" in options
        assert "redact" in options
        assert "truncate" in options
        assert "keep" in options

    def test_add_mask_rule(self) -> None:
        self.page.locator("#mask_field_inp").fill("payload.token")
        self.page.locator("#mask_action_sel").select_option("hash")
        self.page.locator("button[onclick='addMaskRule()']").click()
        expect(self.page.locator("#mask_rules .mask-rule")).to_have_count(1)

    def test_add_multiple_mask_rules(self) -> None:
        for field in ("user.id", "user.email"):
            self.page.locator("#mask_field_inp").fill(field)
            self.page.locator("button[onclick='addMaskRule()']").click()
        expect(self.page.locator("#mask_rules .mask-rule")).to_have_count(2)

    def test_remove_mask_rule(self) -> None:
        self.page.locator("#mask_field_inp").fill("secret.field")
        self.page.locator("button[onclick='addMaskRule()']").click()
        self.page.locator("#mask_rules .mask-rule-remove").first.click()
        expect(self.page.locator("#mask_rules .mask-rule")).to_have_count(0)


# ─────────────────────────────────────────────────────────────────────────────
# Results table
# ─────────────────────────────────────────────────────────────────────────────


class TestResultsTable:
    def test_results_table_present_in_dom(self, ui_page: Page) -> None:
        expect(ui_page.get_by_test_id("results-table")).to_be_attached()

    def test_results_section_hidden_before_search(self, ui_page_with_config: Page) -> None:
        expect(ui_page_with_config.locator("#results")).to_be_hidden()

    @pytest.mark.xfail(reason="Requires live backend returning results")
    def test_results_visible_after_search(self, ui_page_with_search: Page) -> None:
        page = ui_page_with_search
        page.locator("#dsTabs .ds-tab").nth(1).click()
        page.get_by_test_id("search-input").fill("hello")
        page.keyboard.press("Enter")
        page.wait_for_timeout(1000)
        expect(page.locator("#results")).to_be_visible()

    @pytest.mark.xfail(reason="Requires live backend returning results")
    def test_result_rows_rendered(self, ui_page_with_search: Page) -> None:
        page = ui_page_with_search
        page.locator("#dsTabs .ds-tab").nth(1).click()
        page.get_by_test_id("search-input").fill("hello")
        page.keyboard.press("Enter")
        page.wait_for_timeout(1000)
        rows = page.locator("#tbody tr")
        expect(rows).to_have_count(2)


# ─────────────────────────────────────────────────────────────────────────────
# Auth credentials UX
# ─────────────────────────────────────────────────────────────────────────────


class TestAuthCredentials:
    def test_credentials_persist_across_nav(self, ui_page: Page) -> None:
        ui_page.locator("#pk").fill("pk-testkey")
        ui_page.locator("#sk").fill("sk-testkey")
        ui_page.reload()
        # Credentials are stored in localStorage; they should survive a reload
        expect(ui_page.locator("#pk")).to_have_value("pk-testkey")

    def test_hide_auth_inputs_flag(self, page: Page, base_url: str) -> None:
        import json

        def _handle(route: Route) -> None:
            route.fulfill(
                status=200,
                content_type="application/json",
                body=json.dumps({**MOCK_UI_CONFIG, "hide_auth_inputs": True}),
            )

        page.route("**/api/public/ui-config", _handle)
        page.goto(base_url)
        page.wait_for_timeout(300)
        expect(page.locator("#authCard")).to_be_hidden()
