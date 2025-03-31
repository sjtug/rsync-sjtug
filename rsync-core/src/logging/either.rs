use tracing_subscriber::fmt::MakeWriter;
use tracing_subscriber::fmt::writer::EitherWriter;

pub enum EitherMakeWriter<A, B> {
    Left(A),
    Right(B),
}

impl<'a, A, B> MakeWriter<'a> for EitherMakeWriter<A, B>
where
    A: MakeWriter<'a>,
    B: MakeWriter<'a>,
{
    type Writer = EitherWriter<A::Writer, B::Writer>;

    fn make_writer(&'a self) -> Self::Writer {
        match self {
            Self::Left(w) => EitherWriter::A(w.make_writer()),
            Self::Right(w) => EitherWriter::B(w.make_writer()),
        }
    }
}
