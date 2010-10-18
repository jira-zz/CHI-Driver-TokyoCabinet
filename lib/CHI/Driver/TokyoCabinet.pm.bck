package CHI::Driver::TokyoCabinet;

use strict;
use warnings;

use Moose;
use TokyoCabinet;

use CHI::Util qw(read_dir);

use File::Spec;

extends 'CHI::Driver';

our $VERSION = '0.0.1';

has 'filename' => ( is => 'ro', init_arg => undef, lazy_build => 1 );
has 'thdb'     => ( is => 'ro', init_arg => undef, lazy_build => 1 );
has 'root_dir' => ( is => 'ro', required => 1 );

__PACKAGE__->meta->make_immutable();

sub BUILD {
  my ( $self, $params ) = @_;

  $self->{mgr_params} = $self->non_common_constructor_params($params);
}

sub DEMOLISH {
  my $self = shift;

  $self->thdb->close() if $self->{thdb};
}

sub _build_thdb {
  my $self = shift;

  my $thdb = TokyoCabinet::HDB->new();

  die "Cannot instantiate TokyoCabinet::HDB: $@" unless $thdb;

  return $thdb;

}

sub _connect_reader {

  my $self = shift;

  my $filename = $self->filename;


  for ( 1 .. 3 ) {
    if ( $self->thdb->open($filename, $self->thdb->OREADER) ) {
      return 1;
    }
  }

  return 0;
}

sub _connect_writer {

  my $self = shift;

  my $filename = $self->filename;

  for ( 1 .. 3 ) {
    if ( $self->thdb->open($filename, $self->thdb->OWRITER | $self->thdb->OCREAT) ) {
      return 1;
    }
  }

  return 0;
}

sub _disconnect {

  my $self = shift;

     $self->thdb->close() 
}

sub _build_filename {
  my $self = shift;


    my $filename = $self->root_dir .'/'. $self->escape_for_filename( $self->namespace ) . ".tch";


    return $filename;
}



sub fetch {
  my ( $self, $key ) = @_;

  $self->_connect_reader || return undef;
  #die $self->thdb->errmsg( $self->thdb->ecode );


  my $value = $self->thdb->get($key);

  $self->_disconnect;

  return $value;


}

sub store {

  my ( $self, $key, $data ) = @_;

  $self->_connect_writer ||
  die $self->thdb->errmsg( $self->thdb->ecode );

  if (!$self->thdb->put($key, $data)) {

    my $ecode = $self->thdb->ecode();
    die $self->thdb->errmsg($ecode);
}

  $self->_disconnect;

}

sub remove {
  my ( $self, $key ) = @_;

  $self->_connect_writer ||
  die $self->thdb->errmsg( $self->thdb->ecode );

  if (! $self->thdb->out($key) ) {

    my $ecode = $self->thdb->ecode();

    die $self->thdb->errmsg($ecode);

  }
  $self->_disconnect;
}

sub clear {
  my ($self) = @_;

  $self->_connect_writer ||
  die $self->thdb->errmsg( $self->thdb->ecode );

  if(!$self->thdb->vanish()) {
    my $ecode = $self->thdb->ecode();

    die $self->thdb->errmsg($ecode);
}

  $self->_disconnect;
}


sub get_keys {
  my ($self) = @_;

  $self->_connect_reader || return ();
  #die $self->thdb->errmsg( $self->thdb->ecode );

  $self->thdb->iterinit();

  my @keys;
  while(defined(my $key = $self->thdb->iternext())){
    push @keys, $key;
  }
   
  $self->_disconnect;

  return @keys;

}


sub get_namespaces {
  my ($self) = @_;

  my @contents = read_dir( $self->root_dir );
  my @namespaces =
  map { $self->unescape_for_filename( substr( $_, 0, -4 ) ) }
  grep { /\.tch$/ } @contents;
  return @namespaces;
}


1;
