## Release Notes

A non-exhaustive list of what has changed in a more readable form than a commit history.
### v1.0.2
  
  - In eapgs_out.csv , schema type for `claimedits` changed from `integer` to `string` to accommodate `claimedits` possible length of twenty characters.

### v1.0.1

  - Replace encoding errors with `?` when writing to text for application processing.

### v1.0.0

  - Initial release of product component
    - Added `eapg.shared`, which primarily contains tooling for converting PRM-processed datasets into EAPG input format
    - Added `eapg.execution`, which contains tooling for running the EAPG grouper with various options
    - Modified default EAPG input templates to match PRM-processed data specifications
      - Converted to `yyyy-MM-dd` date format
      - Widened character fields to match widths in PRM datamart specifications
      - Expanded `medicalrecordnumber` field to 999 items, to contain `sequencenumber` for merging back to the source claims
