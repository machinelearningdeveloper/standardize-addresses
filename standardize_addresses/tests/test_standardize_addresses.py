import standardize_addresses.standardize_addresses as sa


class TestStandardizeAddresses(object):
    def test_parse_args(self):
        import sys
        old_sys_argv = sys.argv
        commandline_arguments = [
            '--input_file', 'addresses.txt',
            '--output_file', 'standardized_addresses.txt',
            '--address_column', 'ADDRESS',
            '--sep', '|',
            '--chunksize', '1000000']
        sys.argv = [sys.argv[0]] + commandline_arguments
        args = sa.parse_args()
        sys.argv = old_sys_argv
        assert args.input_file == 'addresses.txt'
        assert args.output_file == 'standardized_addresses.txt'
        assert args.address_column == 'ADDRESS'
        assert args.sep == '|'
        assert args.chunksize == 1000000
