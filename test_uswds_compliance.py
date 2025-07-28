#!/usr/bin/env python3
"""
Test script to verify USWDS compliance and functionality of the table filtering/sorting system.
"""

import re
from pathlib import Path

def check_uswds_compliance():
    """Check if templates follow USWDS best practices."""
    
    issues = []
    
    # Check job_table.j2
    table_file = Path("app/templates/components/job-table/job_table.j2")
    if table_file.exists():
        content = table_file.read_text()
        
        # Check for proper USWDS table classes
        if "usa-table" not in content:
            issues.append("Missing usa-table class in job_table.j2")
        
        if "usa-table--striped" not in content:
            issues.append("Missing usa-table--striped class in job_table.j2")
        
        if "usa-table-container--scrollable" not in content:
            issues.append("Missing usa-table-container--scrollable class in job_table.j2")
        
        # Check for proper SVG usage (should use href, not xlink:href)
        if "xlink:href" in content:
            issues.append("Using deprecated xlink:href instead of href in job_table.j2")
        
        # Check for proper button structure
        if "usa-table__header-button" not in content:
            issues.append("Missing usa-table__header-button class in job_table.j2")
    
    # Check job_filters.j2
    filters_file = Path("app/templates/components/job-table/job_filters.j2")
    if filters_file.exists():
        content = filters_file.read_text()
        
        # Check for proper form structure
        if "usa-form" not in content:
            issues.append("Missing usa-form class in job_filters.j2")
        
        if "usa-fieldset" not in content:
            issues.append("Missing usa-fieldset class in job_filters.j2")
        
        if "usa-legend" not in content:
            issues.append("Missing usa-legend class in job_filters.j2")
        
        # Check for proper input classes
        if "usa-input" not in content:
            issues.append("Missing usa-input class in job_filters.j2")
        
        if "usa-select" not in content:
            issues.append("Missing usa-select class in job_filters.j2")
        
        if "usa-label" not in content:
            issues.append("Missing usa-label class in job_filters.j2")
        
        # Check for responsive grid
        if "grid-row" not in content or "grid-col" not in content:
            issues.append("Missing responsive grid classes in job_filters.j2")
    
    # Check job_table_scripts.j2
    scripts_file = Path("app/templates/components/job-table/job_table_scripts.j2")
    if scripts_file.exists():
        content = scripts_file.read_text()
        
        # Check for deprecated xlink:href usage
        if "xlink:href" in content:
            issues.append("Using deprecated xlink:href instead of href in job_table_scripts.j2")
        
        # Check for clearAllFilters function
        if "clearAllFilters" not in content:
            issues.append("Missing clearAllFilters function in job_table_scripts.j2")
        
        # Check for proper focus styling
        if "outline: 0.25rem solid #2491ff" not in content:
            issues.append("Missing USWDS compliant focus styling in job_table_scripts.j2")
    
    return issues

def check_accessibility_features():
    """Check for accessibility features."""
    
    issues = []
    
    # Check job_table.j2 for accessibility
    table_file = Path("app/templates/components/job-table/job_table.j2")
    if table_file.exists():
        content = table_file.read_text()
        
        # Check for proper ARIA attributes
        if 'role="columnheader"' not in content:
            issues.append("Missing role='columnheader' in sortable headers")
        
        if 'aria-hidden="true"' not in content:
            issues.append("Missing aria-hidden='true' on decorative icons")
        
        if 'focusable="false"' not in content:
            issues.append("Missing focusable='false' on decorative icons")
        
        # Check for proper scope attributes
        if 'scope="col"' not in content:
            issues.append("Missing scope='col' on table headers")
        
        # Check for table caption
        if "<caption>" not in content:
            issues.append("Missing table caption for accessibility")
    
    # Check job_filters.j2 for accessibility
    filters_file = Path("app/templates/components/job-table/job_filters.j2")
    if filters_file.exists():
        content = filters_file.read_text()
        
        # Check for proper label association
        if 'for="' not in content:
            issues.append("Missing proper label association in filters")
        
        # Check for fieldset/legend structure
        if "<fieldset>" not in content or "<legend>" not in content:
            issues.append("Missing proper fieldset/legend structure for form grouping")
    
    return issues

def check_javascript_functionality():
    """Check for proper JavaScript functionality."""
    
    issues = []
    
    scripts_file = Path("app/templates/components/job-table/job_table_scripts.j2")
    if scripts_file.exists():
        content = scripts_file.read_text()
        
        # Check for proper event handling
        if "addEventListener" not in content:
            issues.append("Missing proper event listener setup")
        
        # Check for HTMX integration
        if "htmx" not in content:
            issues.append("Missing HTMX integration for dynamic updates")
        
        # Check for sort state management
        if "sort_by" not in content or "sort_order" not in content:
            issues.append("Missing sort state management")
        
        # Check for filter parameter preservation
        if "include:" not in content and "hx-include" not in content:
            issues.append("Missing filter parameter preservation in HTMX requests")
    
    return issues

def main():
    """Run all compliance checks."""
    
    print("🔍 Checking USWDS Compliance and Functionality...")
    print("=" * 60)
    
    all_issues = []
    
    # Run compliance checks
    print("\n📋 USWDS Component Compliance:")
    uswds_issues = check_uswds_compliance()
    if uswds_issues:
        for issue in uswds_issues:
            print(f"  ❌ {issue}")
        all_issues.extend(uswds_issues)
    else:
        print("  ✅ All USWDS component standards met")
    
    # Run accessibility checks
    print("\n♿ Accessibility Features:")
    a11y_issues = check_accessibility_features()
    if a11y_issues:
        for issue in a11y_issues:
            print(f"  ❌ {issue}")
        all_issues.extend(a11y_issues)
    else:
        print("  ✅ All accessibility features present")
    
    # Run JavaScript functionality checks
    print("\n⚡ JavaScript Functionality:")
    js_issues = check_javascript_functionality()
    if js_issues:
        for issue in js_issues:
            print(f"  ❌ {issue}")
        all_issues.extend(js_issues)
    else:
        print("  ✅ All JavaScript functionality present")
    
    print("\n" + "=" * 60)
    if all_issues:
        print(f"❌ Found {len(all_issues)} issue(s) that need attention")
        return 1
    else:
        print("✅ All checks passed! The implementation follows USWDS best practices.")
        return 0

if __name__ == "__main__":
    exit(main())
