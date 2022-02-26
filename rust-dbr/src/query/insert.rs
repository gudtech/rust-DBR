/*
sub sql{
      my $self = shift;

      my $conn   = $self->instance->connect('conn') or return $self->_error('failed to connect');
      my $sql;
      my $optimizer_hints = $self->optimizer_hints ? $self->optimizer_hints->sql($conn) : '';
      my $tables = join(',', map {$_->sql($conn)} @{$self->{tables}} );

      $sql = "INSERT INTO $optimizer_hints$tables (" . join (', ', map { $_->sql( $conn ) } @{$self->fields} ) . ') values ';

      my $ct = 0;
      for my $valueset (@{$self->valuesets}){
            $sql .= ($ct++ ? ',' : '') . '(' . join (',', map { $_->sql( $conn ) } @{$valueset} ) . ')';
      }

      $sql .= ' WHERE ' . $self->{where}->sql( $conn ) if $self->{where};
      $sql .= ' FOR UPDATE'                            if $self->{lock} && $conn->can_lock;
      $sql .= ' LIMIT ' . $self->{limit}               if $self->{limit};

      $self->_logDebug2( $sql );
      return $sql;
}
*/

pub struct Insert {}

impl Insert {
    fn test() {
        //sqlx::query!(format!("select {} from {}", "test", "test"));
    }
}
