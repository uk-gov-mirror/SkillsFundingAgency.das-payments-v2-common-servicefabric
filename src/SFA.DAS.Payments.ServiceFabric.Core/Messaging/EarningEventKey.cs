using System;
using System.Collections.Generic;
using SFA.DAS.Payments.Messages.Common.Events;
using SFA.DAS.Payments.Model.Core;

namespace SFA.DAS.Payments.ServiceFabric.Core.Messaging
{
    public class EarningEventKey
    {
        public long JobId { get; set; }
        public long Ukprn { get; set; }
        public Learner Learner { get; set; }
        public LearningAim LearningAim { get; set; }
        public CollectionPeriod CollectionPeriod { get; set; }
        public string EventType {  get; set;}
        public Guid EarningId { get; set; }
        public virtual string Key => CreateKey();
        public virtual string LogSafeKey => CreateLogSafeKey();

        protected EarningEventKey()
        {

        }

        public EarningEventKey(IPaymentsEvent earningEvent)
        {
            if (earningEvent == null) throw new ArgumentNullException(nameof(earningEvent));
            JobId = earningEvent.JobId;
            Ukprn = earningEvent.Ukprn;
            CollectionPeriod = earningEvent.CollectionPeriod;
            Learner = new Learner
            {
                Uln = earningEvent.Learner.Uln,
                ReferenceNumber = earningEvent.Learner.ReferenceNumber
            };
            LearningAim = new LearningAim
            {
                StartDate = earningEvent.LearningAim.StartDate,
                FrameworkCode = earningEvent.LearningAim.FrameworkCode,
                FundingLineType = earningEvent.LearningAim.FundingLineType,
                Reference = earningEvent.LearningAim.Reference,
                SequenceNumber = earningEvent.LearningAim.SequenceNumber,
                PathwayCode = earningEvent.LearningAim.PathwayCode,
                StandardCode = earningEvent.LearningAim.StandardCode,
                ProgrammeType = earningEvent.LearningAim.ProgrammeType,
                CourseCode = earningEvent.LearningAim.CourseCode ?? string.Empty,
                LearningType = earningEvent.LearningAim.LearningType
            };
            EventType = earningEvent.GetType().Name;
            EarningId = earningEvent.EventId;
        }

        protected virtual string CreateKey()
        {
            if (JobId == 0)  //TODO: Should really be using the FundingPlatform to determine if this is an SLD or AS event, but that property is not available on the base PaymentsEvent class.
                return $@"{EarningId}-{Ukprn}-{CollectionPeriod.AcademicYear}-{CollectionPeriod.Period}-{Learner.Uln}-{Learner.ReferenceNumber}-{LearningAim.Reference}-{LearningAim.CourseCode}-{LearningAim.ProgrammeType}-{LearningAim.StandardCode}-{LearningAim.FrameworkCode}-{LearningAim.PathwayCode}-{LearningAim.FundingLineType}-{LearningAim.SequenceNumber}-{LearningAim.StartDate:G}-{EventType}";
          
            return $@"{JobId}-{Ukprn}-{CollectionPeriod.AcademicYear}-{CollectionPeriod.Period}-{Learner.Uln}-{Learner.ReferenceNumber}-{LearningAim.Reference}-{LearningAim.CourseCode}-{LearningAim.ProgrammeType}-{LearningAim.StandardCode}-{LearningAim.FrameworkCode}-{LearningAim.PathwayCode}-{LearningAim.FundingLineType}-{LearningAim.SequenceNumber}-{LearningAim.StartDate:G}-{EventType}";
        }

        protected virtual string CreateLogSafeKey()
        {
            if (JobId == 0)  //TODO: Should really be using the FundingPlatform to determine if this is an SLD or AS event, but that property is not available on the base PaymentsEvent class.
                return $@"{EarningId}-{CollectionPeriod.AcademicYear}-{CollectionPeriod.Period}-{Learner.ReferenceNumber}-{LearningAim.Reference}-{LearningAim.CourseCode}-{LearningAim.ProgrammeType}-{LearningAim.StandardCode}-{LearningAim.FrameworkCode}-{LearningAim.PathwayCode}-{LearningAim.FundingLineType}-{LearningAim.SequenceNumber}-{LearningAim.StartDate:G}-{EventType}";
            return $@"{JobId}-{CollectionPeriod.AcademicYear}-{CollectionPeriod.Period}-{Learner.ReferenceNumber}-{LearningAim.Reference}-{LearningAim.CourseCode}-{LearningAim.ProgrammeType}-{LearningAim.StandardCode}-{LearningAim.FrameworkCode}-{LearningAim.PathwayCode}-{LearningAim.FundingLineType}-{LearningAim.SequenceNumber}-{LearningAim.StartDate:G}-{EventType}";
        }
    }
}