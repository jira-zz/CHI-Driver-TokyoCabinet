package CHI::t::Driver::TokyoCabinet;

use strict;
use warnings;

use base qw(CHI::t::Driver);

sub testing_driver_class    { 'CHI::Driver::TokyoCabinet' }
sub supports_get_namespaces { 1 }
sub supports_clear { 1 }

my ( $root_dir, $root_dir_initialized );


sub required_modules {
  return { 'TokyoCabinet' => undef,};
}

sub clear_root_dir : Test(setup) {
  $root_dir_initialized = 0;
}




sub new_cache_options {
  my $self = shift;


    # Any other CHI->new parameters for your test driver
    # Generate new temp dir for each test method that needs it;
    # previous temp dir gets cleaned up immediately
    #
    if ( !( $root_dir_initialized++ ) ) {
      $root_dir =
      File::Temp->newdir( "chi-driver-tokyocabinet-XXXX", TMPDIR => 1 );
    }
    return ( $self->SUPER::new_cache_options(), root_dir => $root_dir );
        
}

1;
