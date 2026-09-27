"""The showcase page renders the findings and README without drifting from them."""

from scripts import make_showcase as showcase

FINDINGS = (showcase.REPO_ROOT / "analysis" / "findings.md").read_text(encoding="utf-8")
README = (showcase.REPO_ROOT / "README.md").read_text(encoding="utf-8")


def test_headline_numbers_come_from_the_findings_table():
    numbers = showcase.headline_numbers(FINDINGS)

    assert len(numbers) == 4
    for value, meaning in numbers:
        assert f"| **{value}** | {meaning} |" in FINDINGS


def test_data_window_is_the_findings_date_range():
    window = showcase.data_window(FINDINGS)

    assert window
    assert window in " ".join(FINDINGS.split())


def test_architecture_diagram_is_the_readme_mermaid_block():
    diagram = showcase.readme_mermaid(README)

    assert diagram.startswith("flowchart")
    assert f"```mermaid\n{diagram}\n```" in README


def test_markdown_italic_line_becomes_em():
    html = showcase.markdown_to_html("_7 of 95 headlines were general news and filtered out._")

    assert html == "<p><em>7 of 95 headlines were general news and filtered out.</em></p>"


def test_repository_paths_become_links_and_commands_stay_code():
    html = showcase.inline_html("Full analysis: `analysis/findings.md`, `make bi`.")

    assert (
        f"<a href='{showcase.REPO_URL}/blob/main/analysis/findings.md'>"
        "<code>analysis/findings.md</code></a>"
    ) in html
    assert "<code>make bi</code>" in html
    assert "blob/main/make bi" not in html


def test_single_asterisk_italic_becomes_em_and_bold_still_works():
    html = showcase.inline_html("the cheapest window is *nearly free*, minimum **−€45.87/MWh**")

    assert "<em>nearly free</em>" in html
    assert "<strong>−€45.87/MWh</strong>" in html
    assert "*" not in html
